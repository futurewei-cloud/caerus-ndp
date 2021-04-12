#!/bin/bash

rm -rf build
rm -rf results

pushd tpch-dbgen || (echo "*** failed to find tpch-dbgen" ; exit 1)
make clean || (echo "*** failed build of tpch-dbgen" ; exit 1)
popd
echo "Done cleaning tpch-dbgen"

rm -rf perf-test/build
rm -rf perf-test/lib
rm -rf perf-test/target
rm -rf perf-test/project/target
rm -rf perf-test/project/project
rm -rf perf-test/.bsp
echo "Done cleaning perf-test"

rm -rf spark-sql-perf/lib
rm -rf spark-sql-perf/target
rm -rf spark-sql-perf/project/target
rm -rf spark-sql-perf/project/project
rm -rf spark-sql-perf/.bsp
echo "Done cleaning spark-sql-perf"

echo "Done cleaning benchmark"
