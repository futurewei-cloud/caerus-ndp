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
#DOCKER_INTERACTIVE_RUN="-i -t"
DOCKER_INTERACTIVE_RUN="-i -t -d"

# Check if arguments provided
if [ $# -eq 0 ]
  then
    DOCKER_INTERACTIVE_RUN="-i -t"
fi

for arg do
  shift
  if [ $arg = "--debug" ]; then
    DOCKER_INTERACTIVE_RUN="-i -t"
    continue
  fi  
  set -- "$@" "$arg"
done


CMD="bin/start-hadoop.sh"

HADOOP_HOME=/opt/hadoop/hadoop-${HADOOP_VERSION}

# Create NameNode and DataNode mount points
mkdir -p ${ROOT_DIR}/volume/namenode
mkdir -p ${ROOT_DIR}/volume/datanode0
mkdir -p ${ROOT_DIR}/volume/logs

if [ "$#" -ge 1 ] ; then
  CMD="$@"
fi

docker run --rm=true $DOCKER_INTERACTIVE_RUN \
  -v "${ROOT_DIR}/volume/namenode:/opt/volume/namenode" \
  -v "${ROOT_DIR}/volume/datanode0:/opt/volume/datanode" \
  -v "${ROOT_DIR}/etc/hadoop/core-site.xml:${HADOOP_HOME}/etc/hadoop/core-site.xml" \
  -v "${ROOT_DIR}/etc/hadoop/hdfs-site.xml:${HADOOP_HOME}/etc/hadoop/hdfs-site.xml" \
  -v "${ROOT_DIR}/bin/start-hadoop.sh:${HADOOP_HOME}/bin/start-hadoop.sh" \
  -v "${ROOT_DIR}/plugins:${DOCKER_HOME_DIR}/plugins" \
  -v "${ROOT_DIR}/volume/logs:${HADOOP_HOME}/logs" \
  -w "${HADOOP_HOME}" \
  -e HADOOP_HOME=${HADOOP_HOME} \
  -u "${USER_ID}" \
  --network dike-net \
  --name hadoop-ndp --hostname hadoop-ndp \
  "hadoop-${HADOOP_VERSION}-ndp-${USER_NAME}" ${CMD}

popd
