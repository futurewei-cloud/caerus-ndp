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

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

/** Is a test suite for the V2 datasource.
 *  This test is regardless of API so that this class can be
 *  extended with an overloaded df method to allow multiple types of
 *  configurations to use the same tests.
 *
 */
abstract class DataSourceV2Suite extends QueryTest with SharedSparkSession {
  import testImplicits._
  override def sparkConf: SparkConf = super.sparkConf

  protected val schema = new StructType()
       .add("i", IntegerType, true)
       .add("j", IntegerType, true)
       .add("k", IntegerType, true)

  /** returns a dataframe object, which is to be used for testing of
   *  each test case in this suite.
   *  This can be overloaded in a new suite, which defines
   *  its own data frame.
   *
   * @return DataFrame - The new dataframe object to be used in testing.
   */
  protected def df() : DataFrame

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("TRACE")

    df.createOrReplaceTempView("integers")
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }
  test("simple scan") {
    checkAnswer(df, Seq(Row(0, 5, 1), Row(1, 10, 2), Row(2, 5, 1),
                        Row(3, 10, 2), Row(4, 5, 1), Row(5, 10, 2), Row(6, 5, 1)))
    df.show()
  }
  test("simple project") {
    checkAnswer(df.select("i", "j", "k"),
                Seq(Row(0, 5, 1), Row(1, 10, 2), Row(2, 5, 1),
                    Row(3, 10, 2), Row(4, 5, 1), Row(5, 10, 2), Row(6, 5, 1)))
    checkAnswer(df.select("i", "j"),
                Seq(Row(0, 5), Row(1, 10), Row(2, 5),
                       Row(3, 10), Row(4, 5), Row(5, 10), Row(6, 5)))
    checkAnswer(df.select("i"),
                Seq(Row(0), Row(1), Row(2),
                    Row(3), Row(4), Row(5), Row(6)))
    checkAnswer(df.select("j"),
                Seq(Row(5), Row(10), Row(5),
                    Row(10), Row(5), Row(10), Row(5)))
    checkAnswer(df.select("k"),
                Seq(Row(1), Row(2), Row(1),
                    Row(2), Row(1), Row(2), Row(1)))
    checkAnswer(df.filter("i >= 5"),
                Seq(Row(5, 10, 2), Row(6, 5, 1)))
  }
  test("complex project") {
    checkAnswer(df.select("k", "j", "i"),
                Seq(Row(1, 5, 0), Row(2, 10, 1), Row(1, 5, 2),
                       Row(2, 10, 3), Row(1, 5, 4), Row(2, 10, 5), Row(1, 5, 6)))
    checkAnswer(df.select("k", "i", "j"),
                Seq(Row(1, 0, 5), Row(2, 1, 10), Row(1, 2, 5),
                       Row(2, 3, 10), Row(1, 4, 5), Row(2, 5, 10), Row(1, 6, 5)))
    checkAnswer(df.select("k", "i", "j", "i"),
                Seq(Row(1, 0, 5, 0), Row(2, 1, 10, 1), Row(1, 2, 5, 2),
                       Row(2, 3, 10, 3), Row(1, 4, 5, 4), Row(2, 5, 10, 5), Row(1, 6, 5, 6)))
    checkAnswer(df.select("k", "i", "j", "i", "k"),
                Seq(Row(1, 0, 5, 0, 1), Row(2, 1, 10, 1, 2), Row(1, 2, 5, 2, 1),
                    Row(2, 3, 10, 3, 2), Row(1, 4, 5, 4, 1), Row(2, 5, 10, 5, 2),
                    Row(1, 6, 5, 6, 1)))
  }
}
