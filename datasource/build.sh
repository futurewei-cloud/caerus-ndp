#!/bin/bash
ROOT_DIR=../datasource
if [ ! -d    $ROOT_DIR/lib ]; then
  mkdir $ROOT_DIR/lib
fi
if [ ! -d build ]; then
  mkdir build
fi
#DIKECLIENTJAR=../dikeHDFS/client/ndp-hdfs/target/ndp-hdfs-1.0.jar
#if [ ! -f $DIKECLIENTJAR ]; then
#  echo "Please build dikeHDFS client ($DIKECLIENTJAR) before building spark examples"
#  exit 1
#fi
#cp $DIKECLIENTJAR $ROOT_DIR/lib

# Bring in environment including ${ROOT_DIR} etc.
source ../spark/docker/setup.sh
if [ "$#" -gt 0 ]; then
  if [ "$1" == "debug" ]; then
    docker run --rm -it --name ndp_pushdown_build_debug \
      --network dike-net \
      --mount type=bind,source="$(pwd)"/../datasource,target=/datasource  \
      -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
      -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
      -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
      -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
      -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
      -u "${USER_ID}" \
      --entrypoint /bin/bash -w /datasource \
      spark-build-${USER_NAME}
  fi
else
  docker run --rm -it --name ndp_pushdown_build_debug \
    --network dike-net \
    --mount type=bind,source="$(pwd)"/../datasource,target=/datasource  \
    -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
    -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
    -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
    -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
    -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
    -u "${USER_ID}" \
    --entrypoint /datasource/scripts/build.sh -w /datasource \
    spark-build-${USER_NAME}
fi