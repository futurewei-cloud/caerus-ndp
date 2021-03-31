# caerus-ndp

Setup
=========

```bash
git submodule init  
git submodule update --recursive --progress

docker network create dike-net
```

Clean
================================================
Clean out all build and runtime artifacts including dockers.

```bash
./clean.sh
```

Build this repo
========

```bash
./build.sh
```

Demo test of NDP
======================

This demo script will automatically start the hadoop cluster and spark cluster.
The script will then do a demo of a Spark query with no pushdown followed by a Spark query with pushdown enabled.  The user simply needs to press a key before each test or optionally use -quiet to avoid prompts.

```
# To run demo with prompts
./demo.sh

# Or to avoid prompts
./demo.sh -quiet
```

Finally, bring down the servers.

```
./stop.sh
```

Running a test manually
======================
The demo.sh above is a good example of this.
First, bring up all the server code (hdfs, s3, spark)

```
./start.sh
```

Next, use the test.sh script to run a TPC-H test using spark.  

For example:  To run TPC-H test 6 with pushdown followed by no pushdown.

```
# TPC-H test 6 with pushdown.
test.sh -t 6 --pushdown

# TPC-H test 6 with no pushdown.
test.sh -t 6
```

Finally, bring down the servers.

```
./stop.sh
```
