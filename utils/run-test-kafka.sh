#!/bin/bash

git clone --depth=1 https://github.com/wurstmeister/kafka-docker.git && rm -rf kafka-docker/.git
pushd kafka-docker

docker-compose up -d && docker-compose scale kafka=3

popd