#!/bin/bash

echo "hadoop: removing artifacts"
rm -rf build
rm -rf clients/ndp-hdfs/target
rm -rf plugins/projection-plugin/target
rm -rf volume

source ./config.sh

DOCKER_NAME="hadoop-${HADOOP_VERSION}-ndp"

USER_NAME=${SUDO_USER:=$USER}

echo "hadoop: removing dockers: ${DOCKER_NAME} ${DOCKER_NAME}-${USER_NAME}"
docker rmi ${DOCKER_NAME} ${DOCKER_NAME}-${USER_NAME}

echo "hadoop: cleaning done"
