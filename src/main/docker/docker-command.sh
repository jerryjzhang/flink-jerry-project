#!/bin/sh

docker-compose -f docker-compose-kafka.yml up --detach

docker-compose -f docker-compose-kafka.yml down