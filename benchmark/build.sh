#! /bin/bash
set -e

./build_spark_sql_perf.sh

pushd perf-test
./build.sh
popd

pushd tpch-dbgen/
make
popd
