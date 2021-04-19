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
package com.github.perf

import scala.collection.mutable.ArrayBuffer

import com.typesafe.config._
import org.slf4j.LoggerFactory
import scopt.OParser

import org.apache.spark.sql.{DataFrame, Row, SparkSession, SQLContext}

/** The test configuration, which is mostly obtained through either
 *  defaults or through command line parameters.
 */
case class Config(
    var testNumbers: String = "",
    var testList: ArrayBuffer[Integer] = ArrayBuffer.empty[Integer],
    workers: Int = 1,
    verbose: Boolean = false,
    var host: String = "",
    quiet: Boolean = false,
    pushdown: Boolean = false,
    var datasource: String = "",
    testSuite: String = "tpch",
    format: String = "csv",
    gen: Boolean = false,
    normal: Boolean = false,
    kwargs: Map[String, String] = Map())

/** The performance test, which consists of a configuration
 *  specified through the command line or through defaults.
 *
 */
object PerfTest {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   *  @param args - the array of argument parameters from the command line.
   *
   *  @return Config - the configuration object to use.
   */
  def parseArgs(args: Array[String]): Config = {

    val parser = {
      val builder = OParser.builder[Config]
      import builder._
      OParser.sequence(
        programName("Spark TPC Benchmark"),
        head("tpch-test", "0.1"),
        opt[String]('t', "test")
          .action((x, c) => c.copy(testNumbers = x))
          .text("test numbers. e.g. 1,2-3,4,5-7"),
        opt[Unit]("gen")
          .action((x, c) => c.copy(gen = true))
          .text("generate the database"),
        opt[String]("format")
          .action((x, c) => c.copy(format = x))
          .text("Data source format (csv)"),
        opt[String]("datasource")
          .required()
          .action((x, c) => c.copy(datasource = x))
          .text("Data source (ndp, spark)"),
        opt[Unit]("pushdown")
          .action((_, c) => c.copy(pushdown = true))
          .text("Enable pushdown."),
        opt[String]("host")
          .action((x, c) => c.copy(host = x))
          .text("HDFS hostname"),
        opt[String]('s', "suite")
          .action((x, c) => c.copy(testSuite = x))
          .text("TPC Test suite (tpch (default), tpcds)"),
        opt[Unit]("verbose")
          .action((x, c) => c.copy(verbose = true))
          .text("Enable verbose Spark output (TRACE log level )."),
        opt[Unit]('q', "quiet")
          .action((x, c) => c.copy(quiet = true))
          .text("Limit output (WARN log level)."),
        opt[Unit]("normal")
          .action((x, c) => c.copy(normal = true))
          .text("Normal log output (INFO log level)."),
        help("help").text("prints this usage text")
      )
    }
    // OParser.parse returns Option[Config]
    val optionConfig = OParser.parse(parser, args, Config())
    validateConfig(optionConfig)
  }

  /** Returns a validated configuration object.
   *  Note we also perform some processing of the parameters.
   *  If there is no optional Config object passed in, then
   *  we will return a default Config object.
   *
   * @param optionConfig - The optional configuration to validate
   * @return Config - The config object to use.
   */
  def validateConfig(optionConfig: Option[Config]): Config = {

    val config = optionConfig match {
        case Some(config) =>
          processTestList(config)
          config
        case _ =>
          // arguments are bad, error message will have been displayed
          System.exit(1)
          new Config
    }
    if (!config.gen && (config.testList.length == 0)) {
      log.info("\n\nNot enough arguments. Either --gen or -n must be selected.")
      System.exit(1)
    }
    if (config.host == "") {
        // The perf-test.conf file has a few parameters that
        // contain our defaults.
        val conf = ConfigFactory.load("perf-test")
        config.host = conf.getString("perf-test.host")
    }
    config
  }
  def processTestList(config: Config): Unit = {

    val maxTests = TpcDatabase.getNumTests(config.testSuite)
    if (config.testNumbers == "") {
      config.testNumbers = s"1-$maxTests"
    }
    val ranges = config.testNumbers.split(",")
    for (r <- ranges) {
      val numbers = r.split("-")
      if (numbers.length == 1) {
        config.testList += numbers(0).toInt
      } else if (numbers.length > 1) {
        for (i <- numbers(0).toInt to numbers(1).toInt) {
          config.testList += i
        }
      }
    }
  }
  def setLogLevel(config: Config): Unit = {
    val spark = SparkSession.builder()
      .appName("tpc-test")
      .getOrCreate();
    if (config.verbose) {
      spark.sparkContext.setLogLevel("TRACE")
    } else if (config.quiet) {
      spark.sparkContext.setLogLevel("WARN")
    } else if (config.normal) {
      spark.sparkContext.setLogLevel("INFO")
    }
  }
  /** The main entry point for this performance test.
   *  Here we will parse the args, and create a new database object to
   *  run the test or generate the database for.
   *
   * @param args - The command line args.
   * @return Unit
   */
  def main(args: Array[String]) {
    val config = parseArgs(args)
    setLogLevel(config)

    log.info(s"gen: ${config.gen} test: ${config.testList.mkString(",")}" +
             s" pushdown: ${config.pushdown} datasource: ${config.datasource}" +
             s" format: ${config.format} host: ${config.host}")

    val db = new TpcDatabase(new TpcDbOptions(config.testSuite, config.host,
                                              config.format, config.pushdown,
                                              config.datasource))

    /* Either generate the database or run the test(s)
     */
    if (config.gen) {
      db.genDb
    } else {
      log.info("***  Starting Test Run ***")
      db.runTests(config.testList.toArray)
      log.info("***  Test Run Completed ***")
    }
  }
}
