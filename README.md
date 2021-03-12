# caerus-ndp

Setup
=========

```bash
cd benchmark  
git submodule init  
git submodule update --recursive  

docker network create dike-net
```

To clean out all build or runtime artifacts
================================================

```bash
./clean.sh
```

To build
========

```bash
./build.sh
```

To generate database (tpch)
===========================

1) Start hadoop
- TBD
- Note, hdfs cluster needs to be running, start it manually for now.

2) Start spark

```bash
cd spark
./start_spark.sh
cd ..
```

3) generate database (tpch)

```bash
cd benchmark
./run.sh gen
```

To run tpch test (csv data source)
===================================

```bash
./run.sh run 
```

To run tpch test (pushdown data source)
========================================

```bash
./run.sh run pushdown
```
