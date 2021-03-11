# caerus-ndp

cd benchmark
git submodule init
git submodule update --recursive

# docker network create dike-net

# To clean out all build or runtime artifacts
./clean.sh

# To build
./build.sh

# To generate database (tpch)

# 1) Start hadoop
# - TBD

# 2) Start spark
cd spark
./start_spark.sh
cd ..

# 3) generate database (tpch)
cd benchmark
./run.sh gen

# To run tpch test (csv data source)
./run.sh run 

# To run tpch test (pushdown data source)
./run.sh run pushdown