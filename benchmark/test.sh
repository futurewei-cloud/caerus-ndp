#!/bin/bash
TEST=PerfTest
if [ "$1" == "-d" ]; then
  echo "Debugging"
  shift
  docker exec -it sparkmaster spark-submit --master local \
  --class com.github.perf.${TEST} \
  --conf "spark.jars.ivy=/build/ivy" \
  --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=172.18.0.2:5005" \
  --packages com.github.scopt:scopt_2.12:4.0.0-RC2 \
  --jars /dikeHDFS/client/ndp-hdfs/target/ndp-hdfs-1.0.jar,/build/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/build/hdfs-pushdown-datasource_2.12-0.1.0.jar \
   /build/spark-sql-tests_2.12-1.0.jar $@
else
  docker exec -it sparkmaster spark-submit --master local \
  --class com.github.perf.${TEST} \
  --packages com.github.scopt:scopt_2.12:4.0.0-RC2,com.typesafe:config:1.4.1 \
  --jars /datasource/lib/ndp-hdfs-1.0.jar,/benchmark/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/datasource/target/scala-2.12/ndp-datasource_2.12-0.1.0.jar \
   /benchmark/perf-test/target/scala-2.12/perf-test_2.12-1.0.jar $@
fi
# Add the below to the lines above to override the hostname.
# --conf "spark.driver.extraJavaOptions=-Ddatasource.host=hadoop-ndp" \
