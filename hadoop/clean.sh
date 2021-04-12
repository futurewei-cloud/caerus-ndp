#!/bin/bash

echo "hadoop: removing artifacts"
rm -rf build
rm -rf clients/ndp-hdfs/target
rm -rf plugins/projection-plugin/target
rm -rf volume

# shellcheck source=/dev/null
source ./config.sh

DOCKER_NAME="hadoop-${HADOOP_VERSION}-ndp"

USER_NAME=${SUDO_USER:=$USER}

echo "hadoop: removing dockers: ${DOCKER_NAME} ${DOCKER_NAME}-${USER_NAME}"
DOCKER_RM_CMD="docker rmi ${DOCKER_NAME} ${DOCKER_NAME}-${USER_NAME}"
eval "$DOCKER_RM_CMD"

echo "hadoop: cleaning done"
