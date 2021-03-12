#!/bin/bash
set -e
ROOT_DIR=./perf-test
if [ ! -d ${ROOT_DIR}/lib ]; then
  mkdir ${ROOT_DIR}/lib
fi

SPARK_SQL_JAR=./spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar
if [ ! -f $SPARK_SQL_JAR ]; then
  echo "Please build spark-sql-perf ($SPARK_SQL_JAR) before building scripts"
  exit 1
fi
echo "Copying $SPARK_SQL_JAR -> ${ROOT_DIR}/lib"
cp $SPARK_SQL_JAR ${ROOT_DIR}/lib/

cd ${ROOT_DIR}
# Bring in environment including ${ROOT_DIR} etc.
source ../../spark/docker/setup.sh

if [ "$#" -gt 0 ]; then
  if [ "$1" == "debug" ]; then
    docker run --rm -it --name ndp_perf_test_build \
      --network dike-net \
      --mount type=bind,source="$(pwd)"/../../benchmark,target=/benchmark  \
      -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
      -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
      -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
      -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
      -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
      -u "${USER_ID}" \
      --entrypoint /bin/bash -w /benchmark/perf-test \
      caerus-ndp-spark-base-${USER_NAME}
  fi
else
  docker run --rm -it --name ndp_perf_test_build \
    --network dike-net \
    --mount type=bind,source="$(pwd)"/../../benchmark,target=/benchmark  \
    -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
    -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
    -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
    -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
    -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
    -u "${USER_ID}" \
    --entrypoint /benchmark/scripts/build.sh -w /benchmark/perf-test \
    caerus-ndp-spark-base-${USER_NAME}
fi
