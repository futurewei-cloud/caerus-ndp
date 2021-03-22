#!/usr/bin/env bash

set -e # exit on error

if [ ! -f /opt/volume/namenode/current/VERSION ]; then
    ${HADOOP_PATH}/bin/hdfs namenode -format
fi

echo "Starting Name Node ..."
${HADOOP_PATH}/bin/hdfs --daemon start namenode
echo "Starting Data Node ..."
${HADOOP_PATH}/bin/hdfs --daemon start datanode
echo "Ready to serve"

sleep infinity