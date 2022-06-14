#!/bin/bash

cd "$(dirname "$0")"
docker-compose up -d

# Initialise the MongoDB replica set after 5 seconds
sleep 5
printf "\n\nInitialising the MongoDB replica set...\n\n"
docker exec mongo /scripts/rs-init.sh
