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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import com.databricks.spark.sql.perf.Benchmark
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.tpch.TPCHTables
import com.databricks.spark.sql.perf.tpch.TPCH

import org.slf4j.LoggerFactory

class Database(testName: String, format: String = "csv") {
  
  private val log = LoggerFactory.getLogger(getClass)
  val spark = SparkSession.builder()
      .master("local[1]")
      .appName("gen-db")
      .getOrCreate();
  val sqlContext = new org.apache.spark.sql.SQLContext(spark.sparkContext)
  val rootDir = {
    if (format == "csv") {
      s"hdfs://dikehdfs:9000/${testName}" // root directory for generation
      //s"/benchmark/build/tpch-data/${testName}"   // local filesystem directory.
    } else {
      s"ndphdfs://dikehdfs/${testName}" // root directory for ndp.
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
        dsdgenDir = "/spark-sql-perf/tpcds-kit/tools", // location of dsdgen
        scaleFactor = scaleFactor,
        useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
        useStringForDate = true) // true to replace DateType with StringType
      }
    }

  def createTemporaryTable(name: String, location: String, format: String,
                           schema: StructType): Unit = {
      println(s"Creating temporary table $name using data stored in $location.")
      log.info(s"Creating temporary table $name using data stored in $location.")
      sqlContext.read.format(format).schema(schema).load(location).createOrReplaceTempView(name)
      //sqlContext.sql(s"CREATE TEMPORARY VIEW tpch.${name} AS SELECT * FROM ${name}")
    }
  def createTemporaryTables(location: String, format: String): Unit = {
    
    tables.tables.foreach { table =>
      val tableLocation = s"${location}/${table.name}"
      createTemporaryTable(table.name, tableLocation, format, table.schema)
      //println(s"table: ${table.name}")
      //spark.sql(s"DESCRIBE TABLE EXTENDED ${table.name}").show(900, false)
      //spark.sql(s"SELECT COUNT(*) FROM ${table.name}").show(900, false)
      //spark.sql(s"SELECT * FROM ${table.name}").show(900, false)
    }
  }
  def genDb() {
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
  def getReadBytes() : Long = {
    var stats = FileSystem.getStatistics
    if (stats.containsKey(statsType)) stats.get(statsType).getBytesRead else 0
  }
  def runTest(test: Integer) : Unit = {
    val resultLocation = s"/benchmark/${testName}-results-db" // place to write results
    val iterations = 1 // how many iterations of queries to run.
    val timeout = 24*60*60 // timeout, in seconds.

    spark.sql(s"drop database if exists $databaseName cascade")
    spark.sql(s"create database $databaseName")
    spark.sql(s"use $databaseName")
    tables.createTemporaryTables(rootDir, format)
    spark.sql(s"DESCRIBE DATABASE EXTENDED $databaseName").show()
    //spark.sql(s"SHOW TABLES FROM $databaseName").show()
    //spark.sql(s"SHOW TABLE EXTENDED FROM $databaseName LIKE '*'").show(900, false)
    val startBytes = getReadBytes
    val experimentStatus: Benchmark.ExperimentStatus = {
      if (testName == "tpch") {
        val tpch = new TPCH (sqlContext = sqlContext)
        val queries = tpch.queries.slice(test,test+1)
        tpch.runExperiment(
            queries, 
            iterations = iterations,
            resultLocation = resultLocation,
            forkThread = true)                          
      } else {
        val tpcds = new TPCDS (sqlContext = sqlContext)
        val queries = tpcds.tpcds2_4Queries.slice(test,test+1)
        tpcds.runExperiment(
            queries, 
            iterations = iterations,
            resultLocation = resultLocation,
            forkThread = true)
      }
    }
    experimentStatus.waitForFinish(timeout)
    val totalBytes = getReadBytes - startBytes

    experimentStatus.getCurrentResults 
                .withColumn("Name", substring(col("name"), 2, 100))
                .withColumn("Runtime", (col("parsingTime") + col("analysisTime") + 
                            col("optimizationTime") + col("planningTime") + col("executionTime")) / 1000.0)
                .select("Name", "result", "failure", "Runtime").show(200, false)
  }
  def runTests(testList: Array[Integer]) : Unit = {
    for (i <- testList) {
      runTest(i)
    }
  }
}

object PerfTest {

  def main(args: Array[String]) {
    if (args.length < 1) {
        println("gen-db: [gen | run][csv | pushdown]")
        return
    }
    val action = args(0)
    val test = "tpch"
    val format = if (args.length > 1) args(1) else "csv"
    println(s"action: ${action} test: ${test} format: ${format}")
    val db = new Database(test, format)
    if (action == "gen") {
      db.genDb
    } else {
      val testList: Array[Integer] = Array(0)
      println("***  Starting Test Run ***")
      db.runTests(testList)
      println("***  Test Run Completed ***")
    }
  }
}