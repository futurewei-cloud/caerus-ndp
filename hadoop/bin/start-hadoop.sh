#!/bin/bash

set -e # exit on error

if [ ! -f /opt/volume/namenode/current/VERSION ]; then
    ${HADOOP_HOME}/bin/hdfs namenode -format
fi

export HADOOP_CLASSPATH=~/plugins/projection-plugin/target/projection-plugin-1.0.jar

echo "Starting Name Node ..."
${HADOOP_HOME}/bin/hdfs --daemon start namenode
echo "Starting Data Node ..."
${HADOOP_HOME}/bin/hdfs --daemon start datanode
echo "Ready to serve"

#sleep infinity
/bin/bash


echo "/usr/lib/jvm/java-11-openjdk-amd64/bin/java -Dproc_datanode -Djava.net.preferIPv4Stack=true \
-Dhadoop.security.logger=ERROR,RFAS -Dyarn.log.dir=/opt/hadoop/hadoop-3.2.2/logs \
-Dyarn.log.file=hadoop-peter-datanode-dikehdfs.log -Dyarn.home.dir=/opt/hadoop/hadoop-3.2.2 \
-Dyarn.root.logger=INFO,console -Djava.library.path=/opt/hadoop/hadoop-3.2.2/lib/native \
-Dhadoop.log.dir=/opt/hadoop/hadoop-3.2.2/logs \
-Dhadoop.log.file=hadoop-peter-datanode-dikehdfs.log -Dhadoop.home.dir=/opt/hadoop/hadoop-3.2.2 \
-Dhadoop.id.str=peter -Dhadoop.root.logger=INFO,RFA \
-Dhadoop.policy.file=hadoop-policy.xml org.apache.hadoop.hdfs.server.datanode.DataNode"
