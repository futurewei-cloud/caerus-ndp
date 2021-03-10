#!/bin/bash

rm -rf build

cd tpch-dbgen
make clean
cd ../

rm -rf perf-test/build
rm -rf perf-test/lib
rm -rf perf-test/target
rm -rf perf-test/project/target
rm -rf perf-test/project/project
rm -rf perf-test/.bsp

rm -rf spark-sql-perf/lib
rm -rf spark-sql-perf/target
rm -rf spark-sql-perf/project/target
rm -rf spark-sql-perf/project/project
rm -rf spark-sql-perf/.bsp
