#! /bin/bash
set -e

./build_spark_sql_perf.sh
./build_perf_test.sh

cd tpch-dbgen/
make

cd ..
