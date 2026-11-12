#!/usr/bin/env python3
"""Sinexcel / EnSmart SolarAll inverter (ESS LINK AGN8 dongle) to MQTT collector for Home Assistant.
Read-only. Requires Python 3.11+, no third-party libraries.

    python3 esslink.py --dongle IP [--mqtt IP]

Settings are command-line arguments or the constants below (arguments take precedence).
"""
import argparse
import asyncio
import contextlib
import datetime
import functools
import json
import logging
import math
import signal
import struct
import time
from typing import NamedTuple

__version__ = "1.2.0"

log = logging.getLogger("ess")

# --- Settings (or command-line arguments, see --help) ----------------------------------------------
DONGLE_HOST = ""             # dongle IP (required unless given with --dongle)
DONGLE_PORT = 8888
MQTT_HOST = "127.0.0.1"      # MQTT broker; set its IP if it runs on another host
MQTT_PORT = 1883
MQTT_USER = "ess"
MQTT_PASS = "esslink"         # default password, change it together with the broker
TOPIC_PREFIX = "esslink"     # topics: <prefix>/<dongle id>/state
# ----------------------------------------------------------------------------------------------------
DISC = "homeassistant"        # Home Assistant MQTT discovery prefix

# --- Protocol constants and timings ----------------------------------------------------------------
FRAME_START, FRAME_END = 0x7E, 0x0D     # dongle frame delimiters
FC_DISCRETE_INPUTS, FC_INPUT_REGISTERS = 2, 4   # Modbus read functions (the collector never writes)
READ_HEADER_LEN = 5                     # in a response: unit, fc, address(2), byte count
EXCEPTION_LEN = 3                       # exception response: unit, fc with error bit, code
MQTT_CONNACK = 2                        # MQTT packet type CONNACK
METER_ONLINE_MIN = 0.5                  # meter register value from which the meter counts as online
PING_INTERVAL = 25                      # s; MQTT keepalive is 60 s
STATUS_LOG_INTERVAL = 300               # s between "running: ..." log lines
LONG_SESSION = 120                      # s; after a longer session the retry delay resets


def crc16(d):
    c = 0xFFFF
    for b in d:
        c ^= b
        for _ in range(8):
            c = (c >> 1) ^ 0xA001 if c & 1 else c >> 1
    return c


def frame(direction, mid, mtype, payload):
    body = bytes([direction]) + mid.to_bytes(4, "big") + bytes([mtype]) + len(payload).to_bytes(2, "big") + payload
    return bytes([FRAME_START]) + body + crc16(body).to_bytes(2, "big") + bytes([FRAME_END])


class ModbusError(Exception):
    """The device answered with a Modbus exception; `code` is its code."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


# Exception codes meaning "address or value not supported": such groups are disabled for the rest of the session.
# Other codes (for example 6, busy) are treated as transient.
UNSUPPORTED_CODES = (2, 3)


class Dongle:
    """Dongle connection: login, then sequential reads (one request at a time)."""

    def __init__(self):
        self.r = self.w = None
        self.mid = 0x1000
        self.pending = {}
        self.lock = asyncio.Lock()
        self.hello = asyncio.Event()
        self.hello_payload = b""
        self.error = None

    async def connect(self):
        self.r, self.w = await asyncio.wait_for(asyncio.open_connection(DONGLE_HOST, DONGLE_PORT), 10)
        self.task = asyncio.create_task(self._reader())
        try:
            await asyncio.wait_for(self.hello.wait(), 10)
        except BaseException:
            await self.close()
            raise

    async def _reader(self):
        try:
            while True:
                if (await self.r.readexactly(1))[0] != FRAME_START:
                    continue
                hdr = await self.r.readexactly(8)               # dir, id(4), type, len(2)
                L = int.from_bytes(hdr[6:8], "big")
                rest = await self.r.readexactly(L + 3)
                fr = bytes([FRAME_START]) + hdr + rest
                if rest[-1] != FRAME_END or crc16(fr[1:-3]) != int.from_bytes(fr[-3:-1], "big"):
                    log.warning("кадр з неправильним CRC відкинуто")
                    continue
                d, mid, mtype, pl = hdr[0], int.from_bytes(hdr[1:5], "big"), hdr[5], rest[:L]
                if d == 0:                                       # dongle-initiated
                    if mtype == 1:                               # hello/login -> acknowledge
                        self.hello_payload = pl
                        self.w.write(frame(1, mid, 1, b"\x00"))
                        await self.w.drain()
                        self.hello.set()
                    else:
                        log.debug("кадр донгла типу %02x проігноровано", mtype)
                else:
                    fut = self.pending.get(mid)
                    if fut and not fut.done():
                        fut.set_result(pl)
        except Exception as e:
            self.error = e
            if not isinstance(e, (OSError, asyncio.IncompleteReadError)):     # a bug, not a lost connection
                log.exception("читач донгла зупинився через неочікувану помилку")
            for fut in self.pending.values():
                if not fut.done():
                    fut.set_exception(ConnectionError(repr(e)))

    async def read(self, fc, addr, count, unit=1):
        """Single request path to the dongle: read-only (FC02, FC04)."""
        if fc not in (FC_DISCRETE_INPUTS, FC_INPUT_REGISTERS):
            raise ValueError("дозволені лише функції читання 02/04")
        async with self.lock:
            if self.error:
                raise ConnectionError(repr(self.error))
            self.mid = (self.mid + 1) & 0xFFFFFFFF
            mid = self.mid
            fut = asyncio.get_running_loop().create_future()
            self.pending[mid] = fut
            try:
                self.w.write(frame(0, mid, 5, bytes([unit, fc]) + addr.to_bytes(2, "big") + count.to_bytes(2, "big")))
                await self.w.drain()
                pl = await asyncio.wait_for(fut, 6)
            finally:
                self.pending.pop(mid, None)
        if len(pl) < EXCEPTION_LEN:
            raise ConnectionError(f"надто коротка відповідь донгла ({len(pl)} байт)")
        if pl[1] & 0x80:
            raise ModbusError(pl[2], f"виняток {pl[2]:#04x} для fc{fc:02d} 0x{addr:04x} x{count}")
        want = count * 2 if fc == FC_INPUT_REGISTERS else (count + 7) // 8
        data = pl[READ_HEADER_LEN:READ_HEADER_LEN + pl[READ_HEADER_LEN - 1]] if len(pl) >= READ_HEADER_LEN else b""
        if len(data) != want:
            raise ConnectionError(f"відповідь донгла неповна: {len(data)} байт замість {want} (fc{fc:02d} 0x{addr:04x})")
        return data

    async def close(self):
        with contextlib.suppress(Exception):                     # best-effort shutdown
            self.task.cancel()
            self.w.close()


# --- data model ----------------------------------------------------------------------------------
class Group(NamedTuple):
    """A read group: one Modbus function over a contiguous block."""
    fc: int         # Modbus function: 4 (registers) or 2 (bits)
    base: int       # start address
    count: int      # number of registers (FC04) or bits (FC02)
    unit: int       # device unit
    period: int     # polling period, s


_GROUP_ROWS = {
    "flow":   (4, 0x1270, 16, 1, 10),
    "bat":    (4, 0x1200, 32, 1, 10),
    "grid":   (4, 0x1000, 72, 1, 30),
    "bms":    (4, 0xB100, 96, 1, 60),
    "meter":  (4, 0xA000, 2, 1, 30),
    "meterd": (4, 0xA100, 48, 1, 30),
    "clock":  (4, 0x10F0, 12, 1, 60),
    "energy": (4, 0x1240, 48, 1, 30),
    "alarm1": (2, 0x1048, 256, 1, 30),
    "alarm2": (2, 0x1048, 256, 2, 30),
    "temp":   (4, 0x10DE, 10, 1, 300),
    "set1":   (4, 0x3000, 16, 1, 300),
    "set2":   (4, 0x3040, 16, 1, 300),
    "set3":   (4, 0x30B0, 16, 1, 300),
    "set4":   (4, 0x3148, 8, 1, 300),
}
GROUPS = {name: Group(*row) for name, row in _GROUP_ROWS.items()}
W, V, A, HZ, PCT, KWH, C = "W", "V", "A", "Hz", "%", "kWh", "°C"
class Sensor(NamedTuple):
    """A Home Assistant entity read from a float32 register."""
    key: str
    name: str | None            # None = internal value without an entity
    addr: int
    mult: float                    # multiplier (sign and units)
    unit: str | None
    device_class: str | None
    state_class: str | None
    enabled: int                   # 0 = disabled by default in HA
    category: str | None
    precision: int | None = None   # display precision in HA, where the default rounding is too coarse


_SENSOR_ROWS = [
    ("solar_power", "Потужність PV", 0x1270, 1000, W, "power", "measurement", 1, None),
    ("battery_power", "Потужність батареї", 0x1272, -1000, W, "power", "measurement", 1, None),
    ("load_power", "Потужність навантаження", 0x1274, 1000, W, "power", "measurement", 1, None),
    ("grid_power", "Потужність мережі", 0x1276, -1000, W, "power", "measurement", 1, None),
    ("backup_load_power", "Резервне навантаження", 0x1278, 1000, W, "power", "measurement", 1, None),
    ("pv1_power", "Потужність PV1", 0x1204, 1000, W, "power", "measurement", 0, None),
    ("pv2_power", "Потужність PV2", 0x120A, 1000, W, "power", "measurement", 0, None),
    ("battery_soc", "Заряд батареї (SOC)", 0x1212, 1, PCT, "battery", "measurement", 1, None),
    ("battery_voltage", "Напруга батареї", 0x120C, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("battery_current", "Струм батареї", 0x120E, -1, A, "current", "measurement", 1, "diagnostic"),
    ("battery2_soc", "Заряд батареї 2 (SOC)", 0x121C, 1, PCT, "battery", "measurement", 1, None),
    ("battery2_voltage", "Напруга батареї 2", 0x1216, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("battery2_current", "Струм батареї 2", 0x1218, -1, A, "current", "measurement", 1, "diagnostic"),
    ("battery2_power", "Потужність батареї 2", 0x121A, -1000, W, "power", "measurement", 1, "diagnostic"),
    ("grid_voltage_a", "Напруга мережі L1", 0x1000, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("grid_voltage_b", "Напруга мережі L2", 0x1002, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("grid_voltage_c", "Напруга мережі L3", 0x1004, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("grid_frequency", "Частота мережі", 0x1012, 1, HZ, "frequency", "measurement", 1, "diagnostic", 2),
    ("energy_battery_charge", "Енергія заряду, всього", 0x125E, 1, KWH, "energy", "total_increasing", 1, None),
    ("energy_battery_discharge", "Енергія розряду, всього", 0x1260, 1, KWH, "energy", "total_increasing", 1, None),
    ("energy_solar_today", "Енергія PV за добу", 0x1264, 1, KWH, "energy", "total_increasing", 1, None),
    ("energy_grid_import_today", "Енергія з мережі за добу", 0x1266, 1, KWH, "energy", "total_increasing", 1, None),
    ("energy_grid_export_today", "Енергія в мережу за добу", 0x1268, 1, KWH, "energy", "total_increasing", 1, None),
    ("energy_load_today", "Споживання за добу", 0x126E, 1, KWH, "energy", "total_increasing", 1, None),
    ("temperature_0", "Температура інвертора", 0x10DE, 1, C, "temperature", "measurement", 1, "diagnostic"),
    ("pv1_voltage", "Напруга PV1", 0x1200, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("pv1_current", "Струм PV1", 0x1202, 1, A, "current", "measurement", 1, "diagnostic"),
    ("pv2_voltage", "Напруга PV2", 0x1206, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("pv2_current", "Струм PV2", 0x1208, 1, A, "current", "measurement", 1, "diagnostic"),
    ("grid_current_l1", "Струм мережі L1", 0x1014, 1, A, "current", "measurement", 1, "diagnostic"),
    ("grid_current_l2", "Струм мережі L2", 0x1016, 1, A, "current", "measurement", 1, "diagnostic"),
    ("grid_current_l3", "Струм мережі L3", 0x1018, 1, A, "current", "measurement", 1, "diagnostic"),
    ("grid_power_l1", "Потужність мережі L1", 0x102E, -1000, W, "power", "measurement", 1, "diagnostic"),
    ("grid_power_l2", "Потужність мережі L2", 0x1030, -1000, W, "power", "measurement", 1, "diagnostic"),
    ("grid_power_l3", "Потужність мережі L3", 0x1032, -1000, W, "power", "measurement", 1, "diagnostic"),
    ("grid_apparent_l1", "Повна потужність мережі L1", 0x1028, 1000, "VA", "apparent_power", "measurement", 0, "diagnostic"),
    ("grid_apparent_l2", "Повна потужність мережі L2", 0x102A, 1000, "VA", "apparent_power", "measurement", 0, "diagnostic"),
    ("grid_apparent_l3", "Повна потужність мережі L3", 0x102C, 1000, "VA", "apparent_power", "measurement", 0, "diagnostic"),
    ("grid_reactive_l1", "Реактивна потужність мережі L1", 0x1034, 1000, "var", "reactive_power", "measurement", 0, "diagnostic"),
    ("grid_reactive_l2", "Реактивна потужність мережі L2", 0x1036, 1000, "var", "reactive_power", "measurement", 0, "diagnostic"),
    ("grid_reactive_l3", "Реактивна потужність мережі L3", 0x1038, 1000, "var", "reactive_power", "measurement", 0, "diagnostic"),
    ("grid_pf_l1", "Коефіцієнт потужності L1", 0x1040, 1, None, "power_factor", "measurement", 0, "diagnostic", 2),
    ("grid_pf_l2", "Коефіцієнт потужності L2", 0x1042, 1, None, "power_factor", "measurement", 0, "diagnostic", 2),
    ("grid_pf_l3", "Коефіцієнт потужності L3", 0x1044, 1, None, "power_factor", "measurement", 0, "diagnostic", 2),
    ("battery1_power", "Потужність батареї 1", 0x1210, -1000, W, "power", "measurement", 1, "diagnostic"),
    ("meter_voltage_l1", "Лічильник: напруга L1", 0xA100, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("meter_voltage_l2", "Лічильник: напруга L2", 0xA102, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("meter_voltage_l3", "Лічильник: напруга L3", 0xA104, 1, V, "voltage", "measurement", 1, "diagnostic"),
    ("meter_current_l1", "Лічильник: струм L1", 0xA106, 1, A, "current", "measurement", 1, "diagnostic"),
    ("meter_current_l2", "Лічильник: струм L2", 0xA108, 1, A, "current", "measurement", 1, "diagnostic"),
    ("meter_current_l3", "Лічильник: струм L3", 0xA10A, 1, A, "current", "measurement", 1, "diagnostic"),
    ("meter_power", "Лічильник: потужність", 0xA124, 1000, W, "power", "measurement", 1, "diagnostic"),
    ("grid_line_voltage_ab", "Лінійна напруга L1-L2", 0x1006, 1, V, "voltage", "measurement", 0, "diagnostic"),
    ("grid_line_voltage_bc", "Лінійна напруга L2-L3", 0x1008, 1, V, "voltage", "measurement", 0, "diagnostic"),
    ("grid_line_voltage_ca", "Лінійна напруга L3-L1", 0x100A, 1, V, "voltage", "measurement", 0, "diagnostic"),
    ("battery_soh", "Здоров'я батареї (SOH)", 0xB108, 1, PCT, None, "measurement", 1, "diagnostic"),
    ("battery_cell_voltage_max", "Напруга комірки макс.", 0xB10A, 1, V, "voltage", "measurement", 1, "diagnostic", 3),
    ("battery_cell_voltage_min", "Напруга комірки мін.", 0xB10C, 1, V, "voltage", "measurement", 1, "diagnostic", 3),
    ("battery_cell_temp_max", "Температура комірок макс.", 0xB112, 1, C, "temperature", "measurement", 1, "diagnostic"),
    ("battery_cell_temp_min", "Температура комірок мін.", 0xB114, 1, C, "temperature", "measurement", 1, "diagnostic"),
    ("bms_charge_current_limit", "BMS: ліміт струму заряду", 0xB11A, 1, A, "current", "measurement", 1, "diagnostic"),
    ("bms_discharge_current_limit", "BMS: ліміт струму розряду", 0xB11C, 1, A, "current", "measurement", 1, "diagnostic"),
    ("battery_capacity", "Ємність батареї", 0xB15C, 1, KWH, "energy_storage", "measurement", 1, "diagnostic"),
    ("battery_remaining_energy", "Залишок енергії в батареї", 0xB15E, 1, KWH, "energy_storage", "measurement", 1, "diagnostic"),
    ("work_mode_code", None, 0x300C, 1, None, None, None, 0, None),        # internal, converted to text
    ("batt1_min_soc", "Налаштування: Batt1 мін. SoC", 0x3046, 1, PCT, None, None, 1, "diagnostic"),
    ("batt1_backup_soc", "Налаштування: Batt1 резервний SoC", 0x314A, 1, PCT, None, None, 1, "diagnostic"),
    ("batt1_max_charge", "Налаштування: Batt1 макс. струм заряду", 0x3040, 1, A, "current", None, 1, "diagnostic"),
    ("batt1_max_discharge", "Налаштування: Batt1 макс. струм розряду", 0x3042, 1, A, "current", None, 1, "diagnostic"),
    ("batt2_min_soc", "Налаштування: Batt2 мін. SoC", 0x30B8, 1, PCT, None, None, 0, "diagnostic"),
    ("batt2_backup_soc", "Налаштування: Batt2 резервний SoC", 0x314C, 1, PCT, None, None, 0, "diagnostic"),
    ("batt2_max_charge", "Налаштування: Batt2 макс. струм заряду", 0x30B2, 1, A, "current", None, 0, "diagnostic"),
    ("batt2_max_discharge", "Налаштування: Batt2 макс. струм розряду", 0x30B4, 1, A, "current", None, 0, "diagnostic"),
]
SENSORS = [Sensor(*row) for row in _SENSOR_ROWS]
NOISE = {"power": 5.0, "voltage": 5.0, "current": 0.05}      # PV noise threshold by device class
NON_NEGATIVE = {"solar_power", "pv1_power", "pv2_power", "pv1_voltage", "pv1_current", "pv2_voltage", "pv2_current"}
WORK_MODES = {0: "Самоспоживання", 1: "За розкладом", 2: "Динамічний тариф", 3: "Резерв (Backup)", 4: "Дистанційний",
              5: "Пріоритет заряду від PV", 6: "Пріоритет віддачі в мережу", 7: "Регулювання напруги", 8: "Пікове згладжування"}


@functools.cache
def group_of(addr):
    for name, g in GROUPS.items():
        if g.fc == FC_INPUT_REGISTERS and g.base <= addr and addr + 1 < g.base + g.count:
            return name
    raise KeyError(hex(addr))


class State:
    def __init__(self):
        self.raw = {}           # group -> bytes
        self.vals = {}
        self.alarms = []        # [(unit, cls, text)]
        self.clock_at = None    # local time when the inverter clock was read

    def f32(self, addr):
        g = group_of(addr)
        d = self.raw.get(g)
        if d is None:
            return None
        v = struct.unpack_from(">f", d, (addr - GROUPS[g].base) * 2)[0]
        return v if math.isfinite(v) else None

    def update(self):
        for sensor in SENSORS:
            v = self.f32(sensor.addr)
            if v is not None:
                v *= sensor.mult
                if sensor.device_class == "power_factor":
                    v = abs(v)
                elif sensor.key in NON_NEGATIVE:
                    v = max(0.0, v)                 # negative PV "noise" is not shown
                    if v < NOISE[sensor.device_class]:
                        v = 0.0                     # small night-time PV measurement noise (~0.7 V, <1 W) = zero
                v = round(v, 3) + 0.0               # + 0.0 removes negative zero
            self.vals[sensor.key] = v
        self._derived()
        self._alarms()

    def _derived(self):
        v = self.vals
        code = v.get("work_mode_code")
        v["work_mode"] = WORK_MODES.get(int(code)) if code is not None else None
        hi, lo = v.get("battery_cell_voltage_max"), v.get("battery_cell_voltage_min")
        v["cell_voltage_spread_mv"] = round((hi - lo) * 1000, 1) if hi is not None and lo is not None else None
        cap, soc, floor, load = (v.get(k) for k in ("battery_capacity", "battery_soc", "batt1_min_soc", "load_power"))
        if None in (cap, soc, floor, load):
            v["backup_hours"] = None
        else:      # usable energy above the minimum SoC / current load (estimate)
            v["backup_hours"] = round(min(cap * max(soc - floor, 0) * 10 / max(load, 100), 99), 1)
        m = self.f32(0xA000)
        v["meter_status"] = None if m is None else ("онлайн" if m >= METER_ONLINE_MIN else "офлайн")
        v["clock_offset_min"] = self._clock_offset()

    def _clock_offset(self):
        """Inverter clock minus this host's local time, in minutes (None if the clock was not read)."""
        if self.clock_at is None:
            return None
        ck = [self.f32(a) for a in (0x10F2, 0x10F4, 0x10F6, 0x10F8, 0x10FA)]      # year, month, day, hour, minute
        if None in ck:
            return None
        try:
            inv = datetime.datetime(*(int(x) for x in ck))
        except ValueError:
            return None
        return round((inv - self.clock_at.replace(second=0, microsecond=0)).total_seconds() / 60)

    def _alarms(self):
        al = []
        for g, grp in GROUPS.items():
            d = self.raw.get(g)
            if grp.fc != FC_DISCRETE_INPUTS or not d:
                continue
            for i in range(len(d) * 8):
                if (d[i // 8] >> (i % 8)) & 1:
                    cls, text = ALARMS[i] if i < len(ALARMS) else ("i", f"біт {i}")
                    al.append((grp.unit, cls, text))
        self.alarms = al
        bad = [a for a in al if a[1] in ("f", "w")]
        self.vals["alarm_count"] = len(bad)
        self.vals["problem"] = "ON" if bad else "OFF"
        self.vals["alarms_text"] = ("; ".join(a[2] for a in al) or "немає")[:250]


# --- minimal MQTT client (QoS0, retain, LWT) -----------------------------------------------------
def _s(b):
    return len(b).to_bytes(2, "big") + b


def _rl(n):
    o = b""
    while True:
        d, n = n % 128, n // 128
        o += bytes([d | (0x80 if n else 0)])
        if not n:
            return o


class MQTT:
    def __init__(self, cid, will_topic):
        self.cid, self.will_topic = cid, will_topic
        self.r = self.w = None
        self.sent_at = 0.0          # monotonic time of the last packet sent (for the keepalive)

    async def connect(self):
        self.r, self.w = await asyncio.wait_for(asyncio.open_connection(MQTT_HOST, MQTT_PORT), 10)
        try:
            await self._login()
        except BaseException:
            self.w.close()
            raise
        self.task = asyncio.create_task(self._drain())

    async def _login(self):
        flags, auth = 0x02 | 0x04 | 0x20, b""             # clean session, will, will retain
        if MQTT_USER:                                      # MQTT does not allow a password without a user name
            flags |= 0x80
            auth = _s(MQTT_USER.encode())
            if MQTT_PASS:
                flags |= 0x40
                auth += _s(MQTT_PASS.encode())
        body = _s(b"MQTT") + b"\x04" + bytes([flags]) + (60).to_bytes(2, "big") + _s(self.cid.encode()) \
            + _s(self.will_topic.encode()) + _s(b"offline") + auth
        self.w.write(b"\x10" + _rl(len(body)) + body)
        await self.w.drain()
        self.sent_at = time.monotonic()
        ack = await asyncio.wait_for(self.r.readexactly(4), 10)
        if ack[0] >> 4 != MQTT_CONNACK or ack[3] != 0:
            raise ConnectionError(f"MQTT CONNACK відхилено (код {ack[3]}: 4/5 — невірний логін чи пароль): {ack.hex()}")

    async def _drain(self):
        with contextlib.suppress(Exception):                     # connection dropped: mark it lost below
            while await self.r.read(4096):
                pass
        self.r = None

    async def _send(self, packet):
        if self.r is None:
            raise ConnectionError("MQTT connection lost")
        self.w.write(packet)
        await self.w.drain()
        self.sent_at = time.monotonic()

    async def publish(self, topic, payload, retain=True):
        if isinstance(payload, (dict, list)):
            payload = json.dumps(payload, ensure_ascii=False)
        body = _s(topic.encode()) + payload.encode()
        await self._send(bytes([0x30 | (1 if retain else 0)]) + _rl(len(body)) + body)

    async def ping(self):
        await self._send(b"\xc0\x00")

    async def close(self):
        with contextlib.suppress(Exception):
            self.w.write(b"\xe0\x00")
            await self.w.drain()
            self.task.cancel()
            self.w.close()


# Computed entities: (key, name, icon, extra config fields)
DIAG = {"entity_category": "diagnostic"}
EXTRA = (
    ("work_mode", "Режим роботи", "mdi:cog", {}),
    ("backup_hours", "Автономність батареї", "mdi:battery-clock",
     {"unit_of_measurement": "h", "state_class": "measurement", "suggested_display_precision": 1}),
    ("alarms_text", "Аварії та повідомлення", "mdi:alert-circle-outline", {}),
    ("alarm_count", "Кількість аварій", "mdi:alert", dict(DIAG, state_class="measurement")),
    ("meter_status", "Лічильник електроенергії (зв'язок)", "mdi:meter-electric", DIAG),
    ("clock_offset_min", "Відхилення часу інвертора", "mdi:clock-alert-outline",
     dict(DIAG, unit_of_measurement="min", state_class="measurement")),
    ("cell_voltage_spread_mv", "Розкид напруги комірок", "mdi:battery-heart-variant",
     dict(DIAG, unit_of_measurement="mV", state_class="measurement")),
)


def topic(sn, leaf):
    """Collector topic: <prefix>/<dongle id>/<leaf> (leaf: state, availability)."""
    return f"{TOPIC_PREFIX}/{sn}/{leaf}"


def discovery(sn, inv_serial):
    """MQTT discovery configs for Home Assistant: {topic: config}."""
    dev = {"identifiers": [f"esslink_{sn}"], "name": "EnSmart SolarAll EHV ESS-20kW", "manufacturer": "EnSmart / Sinexcel",
           "model": "SolarAll EHV ESS-20kW (донгл AGN8)", "serial_number": inv_serial or sn, "sw_version": __version__}
    base = {"availability_topic": topic(sn, "availability"), "state_topic": topic(sn, "state"), "device": dev}
    out = {}

    def cfg(kind, key, name, **extra):
        out[f"{DISC}/{kind}/esslink_{sn}/{key}/config"] = dict(
            base, name=name, unique_id=f"esslink_{sn}_{key}", value_template="{{ value_json.get('" + key + "') }}", **extra)

    for sensor in SENSORS:
        if sensor.name:
            extra = {"unit_of_measurement": sensor.unit, "device_class": sensor.device_class,
                     "state_class": sensor.state_class, "entity_category": sensor.category}
            extra = {k: v for k, v in extra.items() if v}
            if not sensor.enabled:
                extra["enabled_by_default"] = False
            if sensor.precision is not None:
                extra["suggested_display_precision"] = sensor.precision
            cfg("sensor", sensor.key, sensor.name, **extra)
    for key, name, icon, extra in EXTRA:
        cfg("sensor", key, name, icon=icon, **extra)
    cfg("binary_sensor", "problem", "Є аварії", device_class="problem", payload_on="ON", payload_off="OFF")
    return out


async def read_serial(dg):
    """Inverter serial number (empty string if it could not be read)."""
    try:
        return (await dg.read(FC_INPUT_REGISTERS, 0xC102, 12)).decode("ascii", "replace").strip("\x00 ")
    except (OSError, TimeoutError, ModbusError) as e:
        log.warning("серійний № не прочитано: %s", e)
        return ""


class Publisher:
    """Broker connection kept across dongle sessions.

    Discovery is published once per broker connection (configs are retained); a dongle reconnect only
    switches availability between "offline" and "online". Any send failure drops the connection,
    and the next `online` call opens a new one.
    """

    def __init__(self):
        self.mq = None
        self.sn = None              # dongle id the connection was opened for (client id and will topic)
        self.announced = None       # inverter serial whose discovery this connection published

    async def online(self, sn, serial):
        """Connect if needed, publish discovery if not yet published on this connection, then "online"."""
        if self.mq and (self.mq.r is None or self.sn != sn):
            await self.drop()
        try:
            if self.mq is None:
                self.mq = MQTT(f"esslink-{sn}", topic(sn, "availability"))
                await self.mq.connect()
                self.sn, self.announced = sn, None
                log.info("MQTT %s:%s: підключено", MQTT_HOST, MQTT_PORT)
            if self.announced != serial:
                for config_topic, config in discovery(sn, serial).items():
                    await self.mq.publish(config_topic, config)
                self.announced = serial
                log.info("MQTT: автооголошення опубліковано")
            await self.mq.publish(topic(sn, "availability"), "online")
        except BaseException:
            await self.drop()
            raise

    async def state(self, sn, vals):
        await self._guarded(self.mq.publish(topic(sn, "state"), vals))

    async def keepalive(self):
        """PINGREQ if nothing was sent for PING_INTERVAL (any packet resets the broker keepalive)."""
        if self.mq and time.monotonic() - self.mq.sent_at > PING_INTERVAL:
            await self._guarded(self.mq.ping())

    async def offline(self, sn):
        """Best effort "offline" at the end of a dongle session; the connection stays open."""
        if self.mq and self.sn == sn:
            with contextlib.suppress(OSError):
                await self._guarded(self.mq.publish(topic(sn, "availability"), "offline"))

    async def idle(self, seconds):
        """Wait between dongle sessions and keep the broker connection alive meanwhile."""
        end = time.monotonic() + seconds
        while (left := end - time.monotonic()) > 0:
            await asyncio.sleep(min(left, PING_INTERVAL))
            try:
                await self.keepalive()
            except OSError as e:
                log.warning("MQTT: з'єднання втрачено, відновимо з наступним сеансом (%s)", e)

    async def _guarded(self, send):
        try:
            await send
        except BaseException:
            await self.drop()
            raise

    async def drop(self):
        if self.mq:
            await self.mq.close()
        self.mq = None


async def poll_groups(dg, st, due, bad):
    """Read the groups that are due; returns True if any group was read."""
    now = time.monotonic()
    changed = False
    for g, grp in GROUPS.items():
        if g in bad or now < due[g]:
            continue
        try:
            st.raw[g] = await dg.read(grp.fc, grp.base, grp.count, grp.unit)
            if g == "clock":
                st.clock_at = datetime.datetime.now()
            changed = True
        except ModbusError as e:
            st.raw.pop(g, None)                                  # do not show stale values
            if e.code in UNSUPPORTED_CODES:
                log.warning("%s: група вимкнена, пристрій її не підтримує (%s)", g, e)
                bad.add(g)
            else:
                log.warning("%s: тимчасова відмова пристрою, повторимо (%s)", g, e)
        due[g] = now + grp.period
    return changed


async def session(pub, once=False):
    """One dongle session: connect, then poll and publish through `pub` (None = no MQTT); ends with an exception on failure.

    With `once`, all groups are read a single time and printed as JSON (diagnostics).
    """
    dg = Dongle()
    await dg.connect()
    sn = dg.hello_payload[:6].hex() or "unknown"
    try:
        st = State()
        serial = await read_serial(dg)
        log.info("донгл %s, інвертор %s: сеанс розпочато", sn, serial)
        due = dict.fromkeys(GROUPS, 0.0)
        bad = set()
        if once:
            await poll_groups(dg, st, due, bad)                  # every group is due on the first pass
            st.update()
            print(json.dumps({"serial": serial, "alarms": st.alarms, **st.vals}, ensure_ascii=False, indent=1))
            return
        if pub:
            await pub.online(sn, serial)
        last_log = time.monotonic()
        while True:
            if await poll_groups(dg, st, due, bad):
                st.update()
                if pub:
                    await pub.state(sn, st.vals)
            if pub:
                await pub.keepalive()
            now = time.monotonic()
            if now - last_log > STATUS_LOG_INTERVAL:
                log.info("працює: SOC=%s PV=%s Вт мережа=%s Вт аварій=%s", st.vals.get("battery_soc"), st.vals.get("solar_power"),
                         st.vals.get("grid_power"), st.vals.get("alarm_count"))
                last_log = now
            await asyncio.sleep(1)
    finally:
        if pub:
            await pub.offline(sn)
        await dg.close()


ALARMS = [
    ("w", "Grid voltage abnormal"),
    ("w", "Grid frequency abnormal"),
    ("w", "Grid phase sequence reversed"),
    ("w", "Grid phase missing"),
    ("f", "Output voltage abnormal"),
    ("w", "Output frequency abnormal"),
    ("f", "Neutral abnormal"),
    ("w", "Ambient temperature high"),
    ("f", "Heatsink temperature high"),
    ("f", "Insulation fault"),
    ("f", "Leakage protection fault"),
    ("f", "Auxiliary supply fault"),
    ("f", "Fan fault"),
    ("f", "Model capacity fault"),
    ("f", "Surge protector abnormal"),
    ("w", "Islanding protection"),
    ("i", "Battery 1 disconnected"),
    ("w", "Battery 1 overvoltage"),
    ("i", "Battery 1 undervoltage"),
    ("w", "Battery 1 discharge terminated"),
    ("f", "Battery 1 polarity reversed"),
    ("i", "Battery 2 disconnected"),
    ("w", "Battery 2 overvoltage"),
    ("i", "Battery 2 undervoltage"),
    ("w", "Battery 2 discharge terminated"),
    ("f", "Battery 2 polarity reversed"),
    ("i", "PV 1 disconnected"),
    ("f", "PV 1 overvoltage"),
    ("i", "PV 1 current sharing abnormal"),
    ("i", "PV 2 disconnected"),
    ("f", "PV 2 overvoltage"),
    ("i", "PV 2 current sharing abnormal"),
    ("f", "DC bus overvoltage"),
    ("f", "DC bus undervoltage"),
    ("f", "DC bus voltage imbalance"),
    ("f", "PV 1 power transistor fault"),
    ("f", "PV 2 power transistor fault"),
    ("f", "Battery 1 power transistor fault"),
    ("f", "Battery 2 power transistor fault"),
    ("f", "Inverter power transistor fault"),
    ("w", "System output overload"),
    ("f", "Inverter overload"),
    ("f", "Inverter overload timeout"),
    ("w", "Battery 1 overload timeout"),
    ("w", "Battery 2 overload timeout"),
    ("f", "Inverter soft start failed"),
    ("w", "Battery 1 soft start failed"),
    ("w", "Battery 2 soft start failed"),
    ("f", "DSP 1 parameter fault"),
    ("f", "DSP 2 parameter fault"),
    ("f", "DSP version incompatibility"),
    ("f", "CPLD version incompatibility"),
    ("f", "CPLD communication fault"),
    ("f", "DSP communication fault"),
    ("f", "Output DC voltage component excessive"),
    ("f", "Output DC current component excessive"),
    ("f", "Relay self test failed"),
    ("f", "Inverter abnormal"),
    ("f", "Grounding fault"),
    ("w", "PV 1 soft start failed"),
    ("w", "PV 2 soft start failed"),
    ("w", "Balancing circuit overload timeout"),
    ("w", "PV 1 overload timeout"),
    ("w", "PV 2 overload timeout"),
    ("w", "PCB temperature high"),
    ("w", "DC converter temperature high"),
    ("f", "Bus slow overvoltage"),
    ("f", "Off grid output voltage abnormal"),
    ("f", "Hardware bus overvoltage"),
    ("f", "Hardware overcurrent"),
    ("f", "DC converter overvoltage"),
    ("f", "DC converter hardware overvoltage"),
    ("f", "DC converter overcurrent"),
    ("f", "DC converter hardware overcurrent"),
    ("f", "DC converter resonant circuit overcurrent"),
    ("f", "PV 1 polarity reversed"),
    ("f", "PV 2 polarity reversed"),
    ("i", "Battery 1 insufficient power"),
    ("i", "Battery 2 insufficient power"),
    ("i", "Battery 1 charging prohibited"),
    ("w", "Battery 1 discharging prohibited"),
    ("i", "Battery 2 charging prohibited"),
    ("w", "Battery 2 discharging prohibited"),
    ("i", "Battery 1 full"),
    ("w", "Battery 1 discharge terminated"),
    ("i", "Battery 2 full"),
    ("w", "Battery 2 discharge terminated"),
    ("w", "Load power overload"),
    ("f", "Leakage self test abnormal"),
    ("w", "Inverter temperature warning"),
    ("w", "Inverter temperature high"),
    ("w", "DC converter temperature warning"),
    ("w", "Parallel communication warning"),
    ("w", "System derating"),
    ("f", "Inverter relay open circuit"),
    ("f", "Inverter relay short circuit"),
    ("w", "PV connection mode incorrect"),
    ("f", "Parallel module missing"),
    ("w", "Parallel module number duplicated"),
    ("w", "Parallel module parameter conflict"),
    ("w", "PV DC arc fault"),
    ("w", "Meter connection abnormal"),
    ("w", "Inverter pulses blocked"),
    ("i", "PV 3 disconnected"),
    ("f", "PV 3 overvoltage"),
    ("i", "PV 3 current sharing abnormal"),
    ("i", "PV 4 disconnected"),
    ("f", "PV 4 overvoltage"),
    ("i", "PV 4 current sharing abnormal"),
    ("f", "PV 3 power transistor fault"),
    ("f", "PV 4 power transistor fault"),
    ("w", "PV 3 soft start failed"),
    ("w", "PV 4 soft start failed"),
    ("w", "PV 3 overload timeout"),
    ("w", "PV 4 overload timeout"),
    ("f", "PV 3 polarity reversed"),
    ("f", "PV 4 polarity reversed"),
    ("w", "Generator voltage abnormal"),
    ("w", "Generator frequency abnormal"),
    ("w", "Generator phase sequence reversed"),
    ("w", "Generator phase missing"),
    ("w", "Lead acid battery temperature abnormal"),
    ("w", "Battery connection mode incorrect"),
    ("w", "Smart load overload"),
    ("i", "Battery 1 at reserve SOC"),
    ("i", "Battery 2 at reserve SOC"),
    ("w", "Grid overload"),
    ("w", "Generator abnormal"),
    ("f", "Bus soft start failed"),
    ("f", "Grid relay open circuit"),
    ("f", "Grid relay short circuit"),
    ("f", "Generator relay open circuit"),
    ("f", "Generator relay short circuit"),
    ("w", "PV insufficient power"),
    ("w", "PV 1 overcurrent"),
    ("w", "PV 2 overcurrent"),
    ("w", "PV 3 overcurrent"),
    ("w", "PV 4 overcurrent"),
    ("w", "Battery 1 overcurrent"),
    ("w", "Battery 2 overcurrent"),
    ("w", "Battery 1 charging pulses blocked"),
    ("w", "Battery 2 charging pulses blocked"),
    ("f", "Load relay open circuit"),
    ("f", "Load relay short circuit"),
    ("f", "EPO emergency stop"),
    ("f", "Line and neutral reversed"),
    ("f", "Internal fan fault"),
]


def parse_args(argv=None):
    ap = argparse.ArgumentParser(description="Збирач даних інвертора (донгл ESS LINK AGN8) -> MQTT / Home Assistant. Лише читання.")
    ap.add_argument("--dongle", default=DONGLE_HOST, metavar="IP[:ПОРТ]", required=not DONGLE_HOST, help="адреса донгла (порт за замовчуванням 8888)")
    ap.add_argument("--mqtt", default=MQTT_HOST, metavar="IP[:ПОРТ]", help="адреса MQTT-брокера (за замовчуванням %(default)s, порт 1883)")
    ap.add_argument("--user", default=MQTT_USER, help="логін MQTT (за замовчуванням %(default)s)")
    ap.add_argument("--password", default=MQTT_PASS, help="пароль MQTT (за замовчуванням %(default)s)")
    ap.add_argument("-v", "--verbose", action="store_true", help="докладний журнал")
    ap.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    ap.add_argument("--once", action="store_true", help=argparse.SUPPRESS)     # diagnostics: read, print, exit
    return ap.parse_args(argv)


def _hostport(s, default_port):
    if s.count(":") == 1:
        h, p = s.split(":")
        return h, int(p)
    return s, default_port


async def main():
    global DONGLE_HOST, DONGLE_PORT, MQTT_HOST, MQTT_PORT, MQTT_USER, MQTT_PASS
    a = parse_args()
    DONGLE_HOST, DONGLE_PORT = _hostport(a.dongle, DONGLE_PORT)
    MQTT_HOST, MQTT_PORT = _hostport(a.mqtt, MQTT_PORT)
    MQTT_USER, MQTT_PASS = a.user, a.password
    logging.basicConfig(level="DEBUG" if a.verbose else "INFO", format="%(asctime)s %(levelname)s %(message)s")
    log.info("esslink %s", __version__)
    task = asyncio.current_task()
    for sig in (signal.SIGTERM, signal.SIGINT):                  # systemd/docker stop: publish "offline" cleanly
        with contextlib.suppress(NotImplementedError, RuntimeError, ValueError):    # e.g. Windows
            asyncio.get_running_loop().add_signal_handler(sig, task.cancel)
    pub = Publisher() if MQTT_HOST and not a.once else None
    backoff = 5
    try:
        while True:
            started = time.monotonic()
            try:
                await session(pub, a.once)
                if a.once:
                    return
            except (OSError, TimeoutError, asyncio.IncompleteReadError, ModbusError) as e:    # expected network and device failures
                log.error("сеанс перервано: %s", f"{type(e).__name__}: {e}" if str(e) else type(e).__name__)
            except Exception:
                log.exception("сеанс перервано через неочікувану помилку")
            if a.once:
                raise SystemExit(1)                              # diagnostics: report the failure instead of retrying
            if time.monotonic() - started > LONG_SESSION:                 # a long session resets the retry delay
                backoff = 5
            await (pub.idle(backoff) if pub else asyncio.sleep(backoff))
            backoff = min(backoff * 2, 60)
    finally:
        if pub:
            await pub.drop()                                     # "offline" is already published by the session


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt, asyncio.CancelledError):
        asyncio.run(main())
