/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.datasource.tests

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/** A basic example, which writes data to hdfs, and then
 *  reads the data back using ndp pushdown of project.
 */
object DatasourceExample {

  private val logger = LoggerFactory.getLogger(getClass)
  private val debug = false
  private val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("example")
    .getOrCreate()

  def ndpExample(args: Array[String]) {
    val host = {
      if (args.length == 0) {
        "hadoop-ndp"
      } else {
        args(0)
      }
    }
    // Write out some sample data.
    SampleData.initData(host, sparkSession)

    val schema = new StructType()
       .add("i", IntegerType, true)
       .add("j", IntegerType, true)
       .add("k", IntegerType, true)

    sparkSession.sparkContext.setLogLevel("INFO")
    import sparkSession.implicits._

    val ndpDF = if (debug) {
        // Optionally we can also disable project.
        sparkSession.read
        .schema(schema)
        .format("pushdown")
        .option("DisableProjectPush", "")
        .load(s"ndphdfs://${host}${SampleData.dataPath}")
    } else {
      val fileName = s"hdfs://${host}:9000/${SampleData.dataPath}"
      logger.info(s"file: $fileName")

      // sparkSession.conf.set("spark.sql.sources.useV1SourceList", "")
      sparkSession.read
      .schema(schema)
      .format("pushdownFile")
      .load(fileName)
    }

    ndpDF.show()
    ndpDF.select("j", "i").show()
    ndpDF.select("k").show()
  }

  def main(args: Array[String]) {
    ndpExample(args)
  }
}

object SampleData {

  val dataPath = "/data/data.csv"
  private val dataValues = Seq((0, 5, 1), (1, 10, 2), (2, 5, 1),
                               (3, 10, 2), (4, 5, 1), (5, 10, 2), (6, 5, 1))

  /** Initializes a data frame with the sample data and
   *  then writes this dataframe out to hdfs.
   * @param host the hdfs hostname to write the data to.
   * @param spark the spark session to use.
   */
  def initData(host: String, spark: SparkSession): Unit = {
    val s = spark
    import s.implicits._
    val testDF = dataValues.toSeq.toDF("i", "j", "k")
    testDF.select("*").repartition(1)
      .write.mode("overwrite")
      .format("csv")
      .option("partitions", "1")
      .save(s"hdfs://${host}:9000/${dataPath}")
    testDF.show()
  }
}
