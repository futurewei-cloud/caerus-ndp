/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.perf

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.databricks.spark.sql.perf.Benchmark
import com.databricks.spark.sql.perf.ExecutionMode
import com.databricks.spark.sql.perf.tpcds.{TPCDS, TPCDSTables}
import com.databricks.spark.sql.perf.tpch.TPCHTables

import org.slf4j.LoggerFactory

/** A TPC-H or TPC-DS database object.  Supports either generating the
 *  database, or runnint tests on it.
 *
 * @param testName - Kind of test to run, either "tpch" or "tpcds"
 * @param format - either "csv" or "pushdown" - To use our own csv data source with pushdown.
 */
class TpcDatabase(testName: String, format: String = "csv") {
  
  private val log = LoggerFactory.getLogger(getClass)
  val spark = SparkSession.builder()
      .master("local[1]")
      .appName("gen-db")
      .getOrCreate();
  import spark.implicits._
  
  val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
  
  val rootDir = {
    if (format == "csv" || format == "parquet") {
      s"hdfs://dikehdfs:9000/${testName}-${format}" // root directory for generation
      //s"/benchmark/build/tpch-data/${testName}"   // local filesystem directory.
    } else {
      s"ndphdfs://dikehdfs/${testName}-csv" // root directory for ndp.
    }
  }

  val statsType = if (format == "csv") "hdfs" else "ndphdfs"
  val databaseName = s"${testName}"  // name of database to create.
  val scaleFactor = "1" // scaleFactor defines the size of the dataset to generate (in GB).
  val tables = {
    if (testName == "tpch") {
      new TPCHTables(sqlContext,
      dbgenDir = "/benchmark/tpch-dbgen", // location of dbgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = true) // true to replace DateType with StringType
    } else {
      new TPCDSTables(sqlContext,
      dsdgenDir = "/benchmark/tpcds-kit/tools", // location of dsdgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = true) // true to replace DateType with StringType
    }
  }
  /** Generates the TPC database to the location specified by rootDir.
   *  Since we are using HDFS, we are generating a single file, which
   *  we know HDFS will map out to its blocks.
   *
   *  @return None
   */
  def genDb(): Unit = {
    // Run:
    log.info(s"Starting genData for ${testName}")
    tables.genData(
        location = rootDir,
        format = format,
        overwrite = true, // overwrite the data that is already there
        partitionTables = false, // create the partitioned fact tables 
        clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files. 
        filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
        tableFilter = "", // "" means generate all tables
        numPartitions = 1) // how many dsdgen partitions to run - number of input tasks.
    
    log.info("genData complete")
    
    // Create the specified database
    spark.sql(s"create database $databaseName")
  }

  /** Returns the number of bytes read from HDFS for this test.
   *
   *  @return Long - Number of bytes directly from hdfs itself.
   */
  def getReadBytes() : Long = {
    var stats = FileSystem.getStatistics
    if (stats.containsKey(statsType)) stats.get(statsType).getBytesRead else 0
  }
  
  /** Runs a test and returns the DataFrame with the results.
   *
   *  @param test - the number of the test to run
   *
   *  @return DataFrame - The test results including runtime, status, and bytes transferred.
   */
  def runTest(test: Int) : DataFrame = {
    val resultLocation = s"/benchmark/${testName}-results-db" // place to write results
    val iterations = 1 // how many iterations of queries to run.
    val timeout = 24*60*60 // timeout, in seconds.

    spark.sql(s"drop database if exists $databaseName cascade")
    spark.sql(s"create database $databaseName")
    spark.sql(s"use $databaseName")
    tables.createTemporaryTables(rootDir, format)
    //spark.sql(s"DESCRIBE DATABASE EXTENDED $databaseName").show()
    //spark.sql(s"SHOW TABLES FROM $databaseName").show()
    //spark.sql(s"SHOW TABLE EXTENDED FROM $databaseName LIKE '*'").show(900, false)
    val startBytes = getReadBytes
    val experimentStatus: Benchmark.ExperimentStatus = {
      if (testName == "tpch") {
        val tpch = new TPCHWritable(sqlContext = sqlContext,
                            ExecutionMode.WriteParquet(s"/benchmark/test-output/${testName}-${format}"))
        val queries = tpch.queries.slice(test - 1,test)
        tpch.runExperiment(
            queries, 
            iterations = iterations,
            resultLocation = resultLocation,
            forkThread = true)                          
      } else {
        val tpcds = new TPCDS(sqlContext = sqlContext)
        val queries = tpcds.tpcds2_4Queries.slice(test - 1,test)
        tpcds.runExperiment(
            queries, 
            iterations = iterations,
            resultLocation = resultLocation,
            forkThread = true)
      }
    }
    experimentStatus.waitForFinish(timeout)
    val totalBytes = getReadBytes - startBytes
    val resDf = experimentStatus.getCurrentResults 
                .withColumn("name", substring(col("name"), 2, 100))
                .withColumn("runtime", (col("parsingTime") + col("analysisTime") + 
                            col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
                .select("name", "result", "failure", "runtime")
    val row = resDf.first
    val rowMap = row.getValuesMap[Any](row.schema.fieldNames)
    val name = rowMap("name").toString
    val df = experimentStatus.getCurrentResults
    val bytesDf = Seq((name, totalBytes)).toSeq.toDF("test","bytes")
    val fullResDf = resDf.join(bytesDf, resDf("name") === bytesDf("test"), "inner").drop("test")
    //println(fullResDf.schema)
    fullResDf
  }
  def showResult(df: DataFrame): Unit = {
    df.select("name", "runtime", "bytes").repartition(1)
      .write.mode("overwrite")
      .format("csv")
      .option("header", "true")
      .option("partitions", "1")
      .save(s"/benchmark/${testName}-result.csv")
    df.show(200, false)
  }
  /** Runs a list of tests and displays the results.
   *  Accumulates the results as the test is running, and displays the
   *  full set of results as each test finishes.
   *
   *  @param testList - the Array[Integer] of test numbers to run.
   *
   *  @return Unit
   */
  def runTests(testList: Array[Integer]) : Unit = {
    val schema = StructType(StructField("name",StringType,true) :: StructField("result",LongType,true) 
                          ::StructField("failure",StructType(StructField("className",StringType,true)
                                                           ::StructField("message",StringType,true) :: Nil),true)
                          ::StructField("runtime",DoubleType,true)
                          ::StructField("bytes",LongType,false) :: Nil )
    var resultDF = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    var index = 0
    for (i <- testList) {
      val df = runTest(i)
      resultDF = resultDF.union(df)
      if (index != testList.length - 1) {
        showResult(resultDF)
      }
      index += 1
    }
    showResult(resultDF)
  }
}

/** The TPCDatabase with global methods.
 *
 */
object TpcDatabase {
  def getNumTests(testName: String): Integer = {
    if (testName == "tpch") {
      22
    } else { // TPCDS 2.4
      104
    }
  }
}