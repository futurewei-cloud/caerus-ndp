#! /bin/bash
set -e

./build_spark_sql_perf.sh || (echo "*** failed to build spark_sql_perf" ; exit 1)

pushd perf-test  || (echo "*** failed pushd of perf-test" ; exit 1)
./build.sh  || (echo "*** failed to build perf-test" ; exit 1)
popd

pushd tpch-dbgen || (echo "*** failed pushd of tpch-dbgen" ; exit 1)
make || (echo "*** failed to build tpch-dbgen" ; exit 1)
popd
