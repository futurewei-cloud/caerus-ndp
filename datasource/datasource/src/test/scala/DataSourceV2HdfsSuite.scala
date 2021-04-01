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
package com.github.datasource.test

import java.io._
import java.net.URI
import java.nio.charset.StandardCharsets

import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.Path

import org.apache.spark.sql.DataFrame

/** Is a suite of testing which exercises the
 *  V2 data source, but using an HDFS API.
 */
class DataSourceV2HdfsSuite extends DataSourceV2Suite {

  private val host = "hadoop-ndp"
  private val dataPath = "/integer-test"

  /** Initializes a data frame with the sample data and
   *  then writes this dataframe out to hdfs.
   */
  def initData(): Unit = {
    val s = spark
    import s.implicits._
    val testDF = dataValues.toSeq.toDF("i", "j", "k")
    testDF.select("*").repartition(1)
      .write.mode("overwrite")
      .format("csv")
      .option("partitions", "1")
      .save(s"hdfs://${host}:9000${dataPath}")
  }
  private val dataValues = Seq((0, 5, 1), (1, 10, 2), (2, 5, 1),
                               (3, 10, 2), (4, 5, 1), (5, 10, 2), (6, 5, 1))
  var initted: Boolean = false
  /** Returns the dataframe for the sample data
   *  read in through the ndp data source.
   */
  override protected def df() : DataFrame = {
    if (!initted) {
      initData()
      initted = true
    }
    spark.read
      .schema(schema)
      .format("pushdown")
      .load(s"ndphdfs://${host}/integer-test")
  }
}
