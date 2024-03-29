#!/bin/bash
easy_curl() {
curl -H "Authorization: 0G3aENqbDlOGkrxVIrx6fg4BaEsHM8wCPUm0an9jJBE=" -H "Content-Type: application/json" http://"$1":29539/api/v1/"${@:2}" | jq
}

setup_personal() {
    easy_curl "$1" interface --data '{"name": "wg0", "private_key": "yCS8OKwXIhzdrdBBnyRKngCQrHJjfmy7dEEKQ5R0xUo=", "listen_port": 51820}'
    easy_curl "$1" interface/wg0/ips -X "PUT" --data '{"ipaddr": ["10.0.0.1/8"]}'
    easy_curl "$1" interface/wg0/status -X "PUT" --data '{"status": "start"}'
}

setup_enterprise() {
    easy_curl "$1" interface --data '{"name": "wg0", "private_key": "yCS8OKwXIhzdrdBBnyRKngCQrHJjfmy7dEEKQ5R0xUo=", "listen_port": 51820}'
    easy_curl "$1" interface/wg0/ips -X "PUT" --data '{"ipaddr": ["100.64.0.1/10"]}'
    easy_curl "$1" interface/wg0/status -X "PUT" --data '{"status": "start"}'
}

unsetup() {
    easy_curl "$1" interface/wg0 -X "DELETE"
}

# $1에 서버 IP 넣기
# setup_enterprise $1
unsetup $1
setup_personal $1