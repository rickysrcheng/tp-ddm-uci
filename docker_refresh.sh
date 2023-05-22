#!/bin/bash
source .env
if [ "$(docker inspect -f '{{.State.Running}}' $PG_CONTAINER_NAME 2>/dev/null)" = "true" ]; then 
    docker stop $PG_CONTAINER_NAME
    docker rm $PG_CONTAINER_NAME
fi
if [ $# -eq 0 ]; then
    docker compose up -d
    sleep 60
fi