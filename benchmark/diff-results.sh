#!/bin/bash
TEST=DiffResults
if [ "$1" == "debug" ]; then
  echo "Debugging"
  shift
  DOCKER_CMD="docker exec -it sparkmaster spark-submit --master local \
  --class com.github.perf.${TEST} \
  --conf spark.jars.ivy=/build/ivy \
  --conf spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=172.18.0.2:5005 \
  --packages com.github.scopt:scopt_2.12:4.0.0-RC2 \
  --jars /datasource/datasource/lib/ndp-hdfs-1.0.jar,/benchmark/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/datasource/datasource/target/scala-2.12/ndp-datasource_2.12-0.1.0.jar\
   /benchmark/perf-test/target/scala-2.12/perf-test_2.12-1.0.jar $*"
else
  DOCKER_CMD="docker exec -it sparkmaster spark-submit --master local[16] \
  --class com.github.perf.${TEST} \
  --packages com.github.scopt:scopt_2.12:4.0.0-RC2 \
  --jars /datasource/datasource/lib/ndp-hdfs-1.0.jar,/benchmark/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/datasource/datasource/target/scala-2.12/ndp-datasource_2.12-0.1.0.jar\
   /benchmark/perf-test/target/scala-2.12/perf-test_2.12-1.0.jar $*"
fi
echo "$DOCKER_CMD"
eval "$DOCKER_CMD"
