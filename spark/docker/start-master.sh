#!/bin/bash

# Include the setup for our cached local directories. (.m2, .ivy2, etc)
source docker/setup.sh

mkdir -p ${ROOT_DIR}/volume/logs
rm -f ${ROOT_DIR}/volume/logs/master*.log

mkdir -p ${ROOT_DIR}/volume/status
rm -f ${ROOT_DIR}/volume/status/MASTER*

CMD="${DOCKER_HOME_DIR}/bin/start-master.sh"
RUNNING_MODE="daemon"

if [ "$#" -ge 1 ] ; then
  CMD="$@"
  RUNNING_MODE="interactive"
fi

if [ $RUNNING_MODE = "interactive" ]; then
  DOCKER_IT="-i -t"
fi

DOCKER_RUN="docker run ${DOCKER_IT} --rm \
  -p 4040:4040 -p 6066:6066 -p 7077:7077 -p 8080:8080 -p 5005:5005 -p 18080:18080 \
  --expose 7001 --expose 7002 --expose 7003 --expose 7004 --expose 7005 --expose 7077 --expose 6066 \
  --name sparkmaster \
  --network dike-net \
  -e MASTER=spark://sparkmaster:7077 \
  -e SPARK_CONF_DIR=/conf \
  -e SPARK_PUBLIC_DNS=localhost \
  --mount type=bind,source=$(pwd)/../benchmark,target=/benchmark \
  --mount type=bind,source=$(pwd)/../datasource,target=/datasource \
  -v $(pwd)/conf/master:/conf  \
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
  eval ${DOCKER_RUN}
else
  eval ${DOCKER_RUN} &
  while [ ! -f ${ROOT_DIR}/volume/status/SPARK_MASTER_STATE ]; do
    sleep 1  
  done

  cat ${ROOT_DIR}/volume/status/SPARK_MASTER_STATE  
fi