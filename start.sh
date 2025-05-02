#!/bin/sh

#small script to start the bus2go server for testing, at the ip address of the device, port 8000

#check if activated python env
#modify the path as needed
python="$PROJECTS/android-apps/bus2go/api/venv/bin/python"
[ "$(which python)" != "$python" ] && source ./venv/bin/activate

ip="$(ip address | awk '/inet.*(wlp.s.|wlan.)/ {print $2}' | sed 's/\/.*//')"

printf "Ip: %s\n" "$ip"
uvicorn src:main.app --host $ip --port 8000
