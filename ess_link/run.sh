#!/command/with-contenv bashio

mqtt_user=""
mqtt_password=""

if bashio::config.has_value 'mqtt_host'; then
    # Own broker from the options
    mqtt=$(bashio::config 'mqtt_host')
    if bashio::config.has_value 'mqtt_user'; then
        mqtt_user=$(bashio::config 'mqtt_user')
    fi
    if bashio::config.has_value 'mqtt_password'; then
        mqtt_password=$(bashio::config 'mqtt_password')
    fi
elif bashio::services.available 'mqtt'; then
    # Broker app (e.g. Mosquitto): the Supervisor provides the connection details
    mqtt="$(bashio::services 'mqtt' 'host'):$(bashio::services 'mqtt' 'port')"
    mqtt_user=$(bashio::services 'mqtt' 'username')
    mqtt_password=$(bashio::services 'mqtt' 'password')
else
    bashio::exit.nok "MQTT-брокер не знайдено: встановіть додаток Mosquitto broker або вкажіть mqtt_host в налаштуваннях."
fi

# Inverters: the single `dongle` option and/or the `inverters` list; one collector per inverter
names=()
dongles=()
if bashio::config.has_value 'dongle'; then
    name=""
    if bashio::config.has_value 'device_name'; then
        name=$(bashio::config 'device_name')
    fi
    names+=("${name}")
    dongles+=("$(bashio::config 'dongle')")
fi
while IFS=$'\037' read -r name dongle; do      # unit separator: unlike a tab it does not merge an empty name away
    names+=("${name}")
    dongles+=("${dongle}")
done < <(bashio::config 'inverters' | jq -r 'select(type == "object") | [.name // "", .dongle // ""] | join("\u001f")')

count=${#dongles[@]}
if [ "${count}" -eq 0 ]; then
    bashio::exit.nok "Вкажіть адресу донгла (dongle) або список інверторів (inverters)."
fi
for i in "${!dongles[@]}"; do
    if [ -z "${dongles[$i]}" ]; then
        bashio::exit.nok "Для кожного інвертора вкажіть адресу донгла."
    fi
done
if [ "${count}" -gt 1 ]; then
    for i in "${!dongles[@]}"; do
        if [ -z "${names[$i]}" ]; then
            bashio::exit.nok "Кожному інвертору потрібна окрема назва: вона стає назвою пристрою в Home Assistant."
        fi
        for j in "${!dongles[@]}"; do
            if [ "${j}" -gt "${i}" ] && { [ "${names[$i]}" = "${names[$j]}" ] || [ "${dongles[$i]}" = "${dongles[$j]}" ]; }; then
                bashio::exit.nok "Назви й адреси донглів інверторів мають бути різними: «${names[$i]}» / ${dongles[$i]}."
            fi
        done
    done
fi

common=(--mqtt "${mqtt}" --user "${mqtt_user}" --password "${mqtt_password}")
if bashio::config.true 'verbose'; then
    common+=(--verbose)
fi

if [ "${count}" -eq 1 ]; then
    args=(--dongle "${dongles[0]}" "${common[@]}")
    if [ -n "${names[0]}" ]; then
        args+=(--name "${names[0]}")
    fi
    bashio::log.info "Запуск збирача (брокер ${mqtt})"
    exec python3 /esslink.py "${args[@]}"
fi

# Several inverters: separate processes, every log line is prefixed with the inverter name
bashio::log.info "Запуск збирачів: ${count} (брокер ${mqtt})"
pids=()
for i in "${!dongles[@]}"; do
    python3 /esslink.py --dongle "${dongles[$i]}" --name "${names[$i]}" "${common[@]}" \
        > >(PREFIX="[${names[$i]}] " awk '{ print ENVIRON["PREFIX"] $0; fflush() }') 2>&1 &
    pids+=($!)
done

stop() {
    kill "${pids[@]}" 2>/dev/null || true
}
trap stop TERM INT

status=0
wait -n || status=$?
stop
wait || true
exit "${status}"
