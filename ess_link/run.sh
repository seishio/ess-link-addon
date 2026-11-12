#!/command/with-contenv bashio

bashio::config.require 'dongle'

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

args=(--dongle "$(bashio::config 'dongle')" --mqtt "${mqtt}" --user "${mqtt_user}" --password "${mqtt_password}")
if bashio::config.true 'verbose'; then
    args+=(--verbose)
fi

bashio::log.info "Запуск збирача (брокер ${mqtt})"
exec python3 /esslink.py "${args[@]}"
