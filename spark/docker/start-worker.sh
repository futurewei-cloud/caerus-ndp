#!/bin/bash

# shellcheck disable=SC1091
source docker/setup.sh

mkdir -p "${ROOT_DIR}/volume/logs"
rm -f "${ROOT_DIR}/volume/logs/worker*.log"

mkdir -p "${ROOT_DIR}/volume/worker/status"
rm -f "${ROOT_DIR}/volume/status/WORKER*"

CMD="${DOCKER_HOME_DIR}/bin/start-worker.sh"
RUNNING_MODE="daemon"

if [ "$#" -ge 1 ] ; then
  CMD="$*"
  RUNNING_MODE="interactive"
fi

if [ $RUNNING_MODE = "interactive" ]; then
  DOCKER_IT="-i -t"
fi

DOCKER_RUN="docker run ${DOCKER_IT} --rm -p 8081:8081 \
  --expose 7012 --expose 7013 --expose 7014 --expose 7015 --expose 8881 \
  --name sparkworker \
  --network dike-net \
  -e SPARK_CONF_DIR=/conf \
      -e SPARK_WORKER_CORES=2 \
      -e SPARK_WORKER_MEMORY=1g \
      -e SPARK_WORKER_PORT=8881 \
      -e SPARK_WORKER_WEBUI_PORT=8081 \
      -e SPARK_PUBLIC_DNS=localhost \
  --mount type=bind,source=$(pwd)/build,target=/build \
  -v ${ROOT_DIR}/conf/worker:/conf \
  -v ${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2 \
  -v ${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg \
  -v ${ROOT_DIR}/build/.sbt:${DOCKER_HOME_DIR}/.sbt \
  -v ${ROOT_DIR}/build/.cache:${DOCKER_HOME_DIR}/.cache \
  -v ${ROOT_DIR}/build/.ivy2:${DOCKER_HOME_DIR}/.ivy2 \
  -v ${ROOT_DIR}/volume/status:/opt/volume/status \
  -v ${ROOT_DIR}/volume/logs:/opt/volume/logs \
  -v ${ROOT_DIR}/bin/:${DOCKER_HOME_DIR}/bin \
  -e RUNNING_MODE=${RUNNING_MODE} \
  -u ${USER_ID} \
  caerus-ndp-spark-base-${USER_NAME} ${CMD}"

    
if [ $RUNNING_MODE = "interactive" ]; then
  eval "${DOCKER_RUN}"
else
  eval "${DOCKER_RUN}" &
  while [ ! -f "${ROOT_DIR}/volume/status/SPARK_WORKER_STATE" ]; do
    sleep 1  
  done

  cat "${ROOT_DIR}/volume/status/SPARK_WORKER_STATE"
fi

