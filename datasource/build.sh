#!/bin/bash
set -e

ROOT_DIR=../datasource
if [ ! -d    $ROOT_DIR/datasource/lib ]; then
  mkdir -p $ROOT_DIR/datasource/lib
fi
if [ ! -d build ]; then
  mkdir build
fi
NDPCLIENTJAR=../hadoop/clients/ndp-hdfs/target/ndp-hdfs-1.0.jar
if [ ! -f $NDPCLIENTJAR ]; then
  echo "Please build dikeHDFS client ($NDPCLIENTJAR) before building spark examples"
  exit 1
fi
cp $NDPCLIENTJAR $ROOT_DIR/datasource/lib
if [ ! -f $ROOT_DIR/datasource/lib/ndp-hdfs-1.0.jar ]; then
  echo "Please copy dikeHDFS client to datasource/lib before building datasource"
  echo "For example: cp dikeHDFS/client/ndp-hdfs/target/ndp-hdfs-1.0.jar datasource/lib"
  exit 1
fi
# Bring in environment including ${ROOT_DIR} etc.
# shellcheck disable=SC1091
source ../spark/docker/setup.sh
if [ "$#" -gt 0 ]; then
  if [ "$1" == "-d" ]; then
    DOCKER_CMD="docker run --rm -it --name ndp_pushdown_build_debug \
      --network dike-net \
      --mount type=bind,source=$(pwd)/../datasource,target=/datasource  \
      -v ${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
      -v ${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
      -v ${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
      -v ${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
      -v ${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
      -u ${USER_ID} \
      --entrypoint /bin/bash -w /datasource \
      caerus-ndp-spark-base-${USER_NAME}"
  fi
else
  DOCKER_CMD="docker run --rm -it --name ndp_pushdown_build_debug \
    --network dike-net \
    --mount type=bind,source=$(pwd)/../datasource,target=/datasource  \
    -v ${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
    -v ${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
    -v ${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
    -v ${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
    -v ${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
    -u ${USER_ID} \
    --entrypoint /datasource/scripts/build.sh -w /datasource \
    caerus-ndp-spark-base-${USER_NAME}"
fi
echo "$DOCKER_CMD"
eval "$DOCKER_CMD"
