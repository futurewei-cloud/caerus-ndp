datasource
==============
The datasource is a Spark V2 datasource, which supports pushdown of projection to
the hadoop plugin module from ../hadoop/plugins.

How to use
============

The demo explained in the top level README.md is a good place to start.

The examples folder also has a simple test that you can run to see this in action.


```
# run the top level build.sh from the root directory of this repo.
./build.sh

# Next, start the services, including hadoop and Spark.
./start.sh

# Finally, run the example to see a simple example of pushdown.
cd examples
./examples.sh
```

To see where the pushdown is occuring, look for these traces:

2021-04-01 17:51:13,106 INFO hdfs.HdfsStore: Pushdown project: 0,1 to ndphdfs partition: HdfsPartition index 0 offset: 0 length: 45 name: ndphdfs://hadoop-ndp:9870/data/data.csv/part-00000-a298e303-a599-415c-afe5-b2778d5f3f5e-c000.csv

Note that the project "0,1" are the indexes of the columns to be included in the query, and all other columns (in this case 2 and above) are to be pruned.

Unit Test
===========

To exercise the datasource, first run build.sh from the top level of the repo.

```
./build.sh
```

Then run the test.sh script from this folder.

```
cd datasource
./test.sh
```

This script will start the hadoop cluster in a local set of dockers.

Then the unit test for the datasource will run, using spark, and using that hadoop cluster
as storage.  The purpose of the unit test is to exercise the pushdown of projects
from spark, through our datasource to hadoop.
