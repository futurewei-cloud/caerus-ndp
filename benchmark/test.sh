#!/bin/bash
TEST=PerfTest
if [ "$#" -lt 1 ]; then
  echo "Usage: --debug --workers # <args for test or --help>"
  exit 1
fi

DEBUG=NO
WORKERS=1
POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -w)
    WORKERS="$2"
    shift # past argument
    shift # past value
    ;;
    -d|--debug)
    DEBUG=YES
    shift # past argument
    ;;
    *)    # unknown option
    POSITIONAL+=("$1") # save it in an array for later
    shift # past argument
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

echo "DEBUG"  = "${DEBUG}"
echo "WORKERS" = "${WORKERS}"
if [ ${DEBUG} == "YES" ]; then
  echo "Debugging"
  shift
  docker exec -it sparkmaster spark-submit --master local \
  --class com.github.perf.${TEST} \
  --conf "spark.jars.ivy=/build/ivy" \
  --conf "spark.driver.extraJavaOptions=-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=172.18.0.2:5005" \
  --packages com.github.scopt:scopt_2.12:4.0.0-RC2,com.typesafe:config:1.4.1 \
  --jars /datasource/datasource/lib/ndp-hdfs-1.0.jar,/benchmark/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/datasource/datasource/target/scala-2.12/ndp-datasource_2.12-0.1.0.jar \
   /benchmark/perf-test/target/scala-2.12/perf-test_2.12-1.0.jar $@
else
  docker exec -it sparkmaster spark-submit --master local[$WORKERS] \
  --class com.github.perf.${TEST} \
  --packages com.github.scopt:scopt_2.12:4.0.0-RC2,com.typesafe:config:1.4.1 \
  --jars /datasource/datasource/lib/ndp-hdfs-1.0.jar,/benchmark/spark-sql-perf/target/scala-2.12/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar,/datasource/datasource/target/scala-2.12/ndp-datasource_2.12-0.1.0.jar \
   /benchmark/perf-test/target/scala-2.12/perf-test_2.12-1.0.jar $@
fi
# Add the below to the lines above to override the hostname.
# --conf "spark.driver.extraJavaOptions=-Ddatasource.host=hadoop-ndp" \
