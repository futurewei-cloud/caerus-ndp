#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e               # exit on error

pushd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)
source ${ROOT_DIR}/config.sh

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

# If this env variable is empty, docker will be started
# in non interactive mode
DOCKER_INTERACTIVE_RUN="-i -t"

CMD="clients/build.sh"

HADOOP_PATH=/opt/hadoop/hadoop-${HADOOP_VERSION}

# By mapping the .m2 directory you can do an mvn install from
# within the container and use the result on your normal
# system.  And this also is a significant speedup in subsequent
# builds because the dependencies are downloaded only once.
mkdir -p ${ROOT_DIR}/build/.m2
mkdir -p ${ROOT_DIR}/build/.gnupg

# Can be used to share data for tests
mkdir -p ${ROOT_DIR}/build/data

if [ "$#" -ge 1 ] ; then
  CMD="$@"
fi

docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  -v "${ROOT_DIR}/etc/hadoop/core-site.xml:${HADOOP_PATH}/etc/hadoop/core-site.xml" \
  -v "${ROOT_DIR}/etc/hadoop/hdfs-site.xml:${HADOOP_PATH}/etc/hadoop/hdfs-site.xml" \
  -v "${ROOT_DIR}/etc/hadoop/core-client.xml:${HADOOP_PATH}/etc/hadoop/core-client.xml" \
  -v "${ROOT_DIR}/bin/start-hadoop.sh:${HADOOP_PATH}/bin/start-hadoop.sh" \
  -v "${ROOT_DIR}/plugins:${DOCKER_HOME_DIR}/plugins" \
  -v "${ROOT_DIR}/clients:${DOCKER_HOME_DIR}/clients" \
  -v "${ROOT_DIR}/build/.m2:${DOCKER_HOME_DIR}/.m2" \
  -v "${ROOT_DIR}/build/.gnupg:${DOCKER_HOME_DIR}/.gnupg" \
  -v "${ROOT_DIR}/build/data:${DOCKER_HOME_DIR}/data" \
  -w "${DOCKER_HOME_DIR}" \
  -e HADOOP_PATH=${HADOOP_PATH} \
  -u "${USER_ID}" \
  --network dike-net \
  "hadoop-${HADOOP_VERSION}-ndp-${USER_NAME}" ${CMD}

popd
