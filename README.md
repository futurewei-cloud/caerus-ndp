caerus-ndp
==============

This is a POC (Proof of Concept) of Near Data Processing (NDP), on Spark
using pushdowns.  This includes a demonstration of
pushdown on Spark of project for HDFS.

NDP is a technique where processing of data is pushed closer to the source of the data in an attempt to leverage locality and limit the a) data transfer and b) processing cycles needed across the set of data.

Our approach will consider use cases on Spark, and specifically focus on the use cases where Spark is processing data using SQL (Structured Query Language).  This is typical of data stored in a tabular format, and now very popular in Big Data applications and analytics.  Today, these Spark cases use SQL queries, which operate on tables (see background for more detail).  These cases require reading very large tables from storage in order to perform relational queries on the data.

Our approach will consist of pushing down portions of the SQL query to the storage itself so that the storage can perform the operations of project and thereby limit the data transfer of these tables back to the compute node.  This limiting of data transfer also has a secondary benefit of limiting the amount of data needing to be processed on the compute node after the fetch of data is complete.

This repo has everything needed to demonstrate this capability on a single
machine using dockers.  Everything from the build to the servers, to running
the test are all utilizing dockers.

Components
===========

<B>spark</B> - A spark docker is built by this repo.<BR><BR>
<B>datasource</B> - This is a new Spark V2 datasource, which has support for
                      pushdown of project on HDFS.  This datasource utilizes
                      the hadoop NDP client to access the project plugin functionality.<BR><BR>
<B>hadoop</B> A hadoop docker is built by this repo. <BR><BR>
<B>hadoop NDP client</B> This new client provides an API to send a query string to our hadoop project plugin<BR><BR>
<B>hadoop project plugin</B> This new hadoop plugin implements project against .csv files. <BR><BR>

Tools
=======
There are a few tools we are using to demonstrate the above functionality.  
<B>perf-test</B> This is our own small test app, which uses spark-sql-perf to run TPC-H tests.
                 This also contains support for using our own datasource.
<B>spark-sql-perf</B> This is a tool from databricks, which supports TPC-H performance tests.  
                      We are using our own fork of this tool, which merely has a few minor fixes.<BR><BR>
<B>tpch-dbgen</B> - We are using this tool which generates databased for TPC-H.
                      The spark-sql-perf repo requires this tool. We are using our own fork of this tool, which has a one line fix to allow it to be used by spark-sql-perf.<BR><BR>


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
