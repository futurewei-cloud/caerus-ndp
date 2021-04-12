#!/bin/bash
set -e

# Bring in environment including ${ROOT_DIR} etc.
# shellcheck disable=SC1091
source ../spark/docker/setup.sh

if [ "$#" -gt 0 ]; then
  if [ "$1" == "-d" ]; then
    DOCKER_CMD="docker run --rm -it --name ndp_spark_sql_perf_build \
      --network dike-net \
      --mount type=bind,source=$(pwd)/../benchmark,target=/benchmark  \
      -v ${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
      -v ${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
      -v ${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
      -v ${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
      -v ${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
      -u ${USER_ID} \
      --entrypoint /bin/bash -w /benchmark/spark-sql-perf \
      caerus-ndp-spark-base-${USER_NAME}"
  fi
else
  DOCKER_CMD="docker run --rm -it --name ndp_spark_sql_perf_build \
    --network dike-net \
    --mount type=bind,source=$(pwd)/../benchmark,target=/benchmark  \
    -v ${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
    -v ${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
    -v ${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
    -v ${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
    -v ${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
    -u ${USER_ID} \
    --entrypoint /benchmark/scripts/build.sh -w /benchmark/spark-sql-perf\
    caerus-ndp-spark-base-${USER_NAME}"
fi
echo "$DOCKER_CMD"
eval "$DOCKER_CMD"
