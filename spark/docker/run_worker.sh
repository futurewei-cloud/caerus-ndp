#!/bin/bash

source docker/setup.sh

docker run --rm -p 8081:8081 \
  --expose 7012 --expose 7013 --expose 7014 --expose 7015 --expose 8881 \
  --name sparkworker \
  --network dike-net \
  -e "SPARK_CONF_DIR=/conf" \
      -e "SPARK_WORKER_CORES=2" \
      -e "SPARK_WORKER_MEMORY=1g" \
      -e "SPARK_WORKER_PORT=8881" \
      -e "SPARK_WORKER_WEBUI_PORT=8081" \
      -e "SPARK_PUBLIC_DNS=localhost" \
  --mount type=bind,source="$(pwd)"/build,target=/build \
  -v "$(pwd)"/conf/worker:/conf \
  -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
  -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
  -v "${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt" \
  -v "${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache" \
  -v "${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2" \
  -u "${USER_ID}" \
  "spark-run-rel-${USER_NAME}" bin/spark-class org.apache.spark.deploy.worker.Worker spark://sparkmaster:7077

