#!/bin/bash

echo "Starting  servers"
pushd ../hadoop || (echo "*** pushd hadoop failed with $?" ; exit 1)
./start.sh || (echo "*** hadoop start failed with $?" ; exit 1)
# Wait for hdfs to start before we disable safe mode.
# this allows writes to hdfs within the 20 seconds after starting.
sleep 30
../hadoop/disable-safe-mode.sh || (echo "*** disable hadoop safe mode failed with $?" ; exit 1)
popd

echo "Starting datasource test"
# Bring in environment including ${ROOT_DIR} etc.
# shellcheck disable=SC1091
source ../spark/docker/setup.sh

echo "testing datasource"

DOCKER_CMD="docker run --rm -it --name ndp_pushdown_unit_test \
  --network dike-net \
  --mount type=bind,source=$(pwd)/../datasource,target=/datasource  \
  -v ${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
  -v ${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
  -v ${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
  -v ${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
  -v ${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
  -u ${USER_ID} \
  --entrypoint /datasource/scripts/test.sh -w /datasource \
  caerus-ndp-spark-base-${USER_NAME}"

echo "$DOCKER_CMD"
eval "$DOCKER_CMD"

echo "Stopping servers"
pushd ../hadoop
./stop.sh
popd
