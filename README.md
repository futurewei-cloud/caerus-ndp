# caerus-ndp

Setup
=========

```bash
cd benchmark  
git submodule init  
git submodule update --recursive  

docker network create dike-net
```

Clean out all build or runtime artifacts
================================================

```bash
./clean.sh
```

Build this repo
========

```bash
./build.sh
```

Generate database (tpch)
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

run tpch test (csv data source)
===================================

```bash
./run.sh run 
```

run tpch test (pushdown data source)
========================================

```bash
./run.sh run pushdown
```
