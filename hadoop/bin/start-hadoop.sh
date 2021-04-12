#!/bin/bash

set -e # exit on error

if [ ! -f /opt/volume/namenode/current/VERSION ]; then
    "${HADOOP_HOME}/bin/hdfs" namenode -format
fi

export HADOOP_CLASSPATH=${HADOOP_HOME}/plugins/projection-plugin/target/projection-plugin-1.0.jar

echo "Starting Name Node ..."
"${HADOOP_HOME}/bin/hdfs" --daemon start namenode
echo "Starting Data Node ..."
"${HADOOP_HOME}/bin/hdfs" --daemon start datanode

echo "HADOOP_READY"
echo "HADOOP_READY" > /opt/volume/status/HADOOP_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi

