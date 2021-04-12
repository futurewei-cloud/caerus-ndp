#!/bin/bash
bin/spark-class org.apache.spark.deploy.master.Master > /opt/volume/logs/master.log 2>&1 &

echo "SPARK_MASTER_READY"
echo "SPARK_MASTER_READY" > /opt/volume/status/SPARK_MASTER_STATE

echo "RUNNING_MODE $RUNNING_MODE"

if [ "$RUNNING_MODE" = "daemon" ]; then
    sleep infinity
fi