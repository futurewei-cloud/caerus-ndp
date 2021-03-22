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

cd "$(dirname "$0")" # connect to root

ROOT_DIR=$(pwd)
DOCKER_DIR=docker
DOCKER_FILE="${DOCKER_DIR}/Dockerfile"

USER_NAME=${SUDO_USER:=$USER}
USER_ID=$(id -u "${USER_NAME}")

# Set the home directory in the Docker container.
DOCKER_HOME_DIR=${DOCKER_HOME_DIR:-/home/${USER_NAME}}

# If this env variable is empty, docker will be started
# in non interactive mode
#DOCKER_INTERACTIVE_RUN="-i -t"
DOCKER_INTERACTIVE_RUN="-i -t -d"


CMD="/bin/bash"

HADOOP_PATH=/opt/hadoop/hadoop-3.2.2

# Create NameNode and DataNode mount points
mkdir -p ${ROOT_DIR}/volume/namenode
mkdir -p ${ROOT_DIR}/volume/datanode0

if [ "$#" -ge 1 ] ; then
  CMD="$@"
fi

docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  -v "${ROOT_DIR}/volume/namenode:/opt/volume/namenode" \
  -v "${ROOT_DIR}/volume/datanode0:/opt/volume/datanode" \
  -v "${ROOT_DIR}/etc/hadoop/core-site.xml:${HADOOP_PATH}/etc/hadoop/core-site.xml" \
  -v "${ROOT_DIR}/etc/hadoop/hdfs-site.xml:${HADOOP_PATH}/etc/hadoop/hdfs-site.xml" \
  -v "${ROOT_DIR}/bin/start-hadoop.sh:${HADOOP_PATH}/bin/start-hadoop.sh" \
  -w "${HADOOP_PATH}" \
  -e HADOOP_PATH=${HADOOP_PATH} \
  -u "${USER_ID}" \
  --network dike-net \
  --name hadoop-ndp --hostname hadoop-ndp \
  "hadoop-ndp-${USER_NAME}" ${CMD}

#"$@"

#--hostname master