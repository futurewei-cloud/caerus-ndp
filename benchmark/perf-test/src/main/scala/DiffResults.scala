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

import scala.collection.mutable.ArrayBuffer
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.databricks.spark.sql.perf.Benchmark
import com.databricks.spark.sql.perf.tpcds.TPCDSTables
import com.databricks.spark.sql.perf.tpcds.TPCDS
import com.databricks.spark.sql.perf.tpch.TPCHTables
import com.databricks.spark.sql.perf.tpch.TPCH
import com.databricks.spark.sql.perf._

import org.slf4j.LoggerFactory
import scopt.OParser

/** The configuration for the Diff app, which is obtained through either
 *  defaults or through command line parameters.
 */
case class DiffConfig(
    var testNumbers: String = "",
    baselinePath: String = "",
    resultsPath: String = "",
    format: String = "csv",
    test: String = "tpch",
    var testList: ArrayBuffer[Integer] = ArrayBuffer.empty[Integer],
    verbose: Boolean = false,
    quiet: Boolean = true,
    gen: Boolean = false,
    normal: Boolean = false,
    kwargs: Map[String, String] = Map())

/** The diff app, which consists of a configuration
 *  specified through the command line or through defaults.
 *
 */
object DiffResults {

  private val log = LoggerFactory.getLogger(getClass)

  /**
   *  @param args - the array of argument parameters from the command line.
   *
   *  @return DiffConfig - the configuration object to use.
   */
  def parseArgs(args: Array[String]): DiffConfig = {
    
    val builder = OParser.builder[DiffConfig]
    val parser = {
      import builder._
      OParser.sequence(
        programName("Diff TPC Benchmark Results"),
        head("tpch-test", "0.1"),
        opt[String]('t', "test")
          .action((x, c) => c.copy(testNumbers = x))
          .text("test numbers"),
        opt[String]('b', "baseline")
          .action((x, c) => c.copy(baselinePath = x))
          .text("baseline path"),
        opt[String]('r', "results")
          .action((x, c) => c.copy(resultsPath = x))
          .text("results directory"),
        opt[String]('f', "format")
          .action((x, c) => c.copy(format = x))
          .text("format (csv, parquet)"),
        opt[Unit]("verbose")
          .action((x, c) => c.copy(verbose = true))
          .text("Enable verbose Spark output (TRACE log level )."),
        opt[Unit]('q', "quiet")
          .action((x, c) => c.copy(quiet = true))
          .text("Limit Spark output (Spark WARN log level)."),
        opt[Unit]("normal")
          .action((x, c) => c.copy(normal = true))
          .text("Normal Spark output (INFO log level)."),
        help("help").text("prints this usage text"),
      )
    }
    // OParser.parse returns Option[DiffConfig]
    val optionConfig = OParser.parse(parser, args, DiffConfig())
    validateConfig(optionConfig)
  }

  /** Returns a validated configuration object.
   *  Note we also perform some processing of the parameters.
   *  If there is no optional DiffConfig object passed in, then
   *  we will return a default DiffConfig object.
   *
   * @param optionConfig - The optional configuration to validate
   * @return DiffConfig - The config object to use.
   */
  def validateConfig(optionConfig: Option[DiffConfig]): DiffConfig = {
            
    val config = optionConfig match {
        case Some(config) =>
          if (config.testNumbers == "") {
            config.testNumbers = if (config.test == "tpch") "1-22" else "1-104"
          }
          val ranges = config.testNumbers.split(",")
          for (r <- ranges) {
            if (r.contains("-")) {
              val numbers = r.split("-")
              if (numbers.length == 2) {
                for (i <- numbers(0).toInt to numbers(1).toInt) {
                  config.testList += i
                }
              }
            } else {
              config.testList += r.toInt
            }
          }
          config
        case _ =>
          // arguments are bad, error message will have been displayed
          System.exit(1)
          new DiffConfig
    }    
    config
  }
  var successCount = 0
  var failureCount = 0
  var skippedCount = 0
  val spark = SparkSession.builder
                          .master("local")
                          .appName("diffResults")
                          .getOrCreate()

  /** Returns the test directory, comprised of 
   *  the base directory, test name and the config.
   * @param config - test configuration
   * @param test - integer index of the test
   * @param baseDir - root directory for test results.
   * @return String - test directory
   */
  def getTestDir(config: DiffConfig,
                 test: Integer, baseDir: String): String = {
    baseDir + s"/Q${test}.${config.format}"
  }

  /** Performs a diff of the test results from two different
   *  directories, across all the tests that we were asked to diff.
   *
   * @param config - test configuration
   * @return Unit
   */
  def sparkDiffResults(config: DiffConfig): Unit = {
    for (test <- config.testList) {
      val basePath = getTestDir(config, test, config.baselinePath)
      val resultsPath = getTestDir(config, test, config.resultsPath)
      if (scala.reflect.io.File(basePath).exists &&
          scala.reflect.io.File(resultsPath).exists) {
        sparkDiff(config, basePath, resultsPath)
      } else {
        skippedCount += 1
        log.warn(s"skipped: ${basePath} vs ${resultsPath}.")
      }
    }
  }

  /** Reads dataframes from two different directories and
   *  performs a diff.  If differences are found, then are displayed.
   *
   * @param config - test configuration
   * @return Unit
   */
  def sparkDiff(config: DiffConfig, basePath: String, comparePath: String): Unit = {
    
    val baseDf = spark.read.format(config.format).load(basePath)
    val compareDf = spark.read.format(config.format).load(comparePath)

    val differences = baseDf.unionAll(compareDf).except(baseDf.intersect(compareDf)).count
    if (differences != 0) {
      log.warn(s"Found differences for: ${basePath} and ${comparePath}")
      baseDf.show()
      compareDf.show()
      failureCount += 1
    } else {
      log.warn(s"${basePath} and ${comparePath} are the same.")
      successCount += 1
    }
  }    

  /** Sets the loglevel based upon the parameters in the config.
   *
   * @param config - test configuration
   * @return Unit
   */
  def setLogLevel(config: DiffConfig): Unit = {
    if (config.verbose) {
      spark.sparkContext.setLogLevel("TRACE")
    } else if (config.quiet) {
      spark.sparkContext.setLogLevel("WARN")
    } else if (config.normal) {
      spark.sparkContext.setLogLevel("INFO")
    }
  }  

  /** Shows the summary of comparison results
   *
   * @return Unit
   */
  def showResults(): Unit = {
    println(s"success: ${successCount}")
    println(s"failure: ${failureCount}")
    println(s"skipped: ${skippedCount}")
  }

  /** The main entry point for this diff.
   *
   * @param args - The command line args.
   * @return Unit
   */
  def main(args: Array[String]) {
    val config = parseArgs(args)
    setLogLevel(config)
    sparkDiffResults(config)
    showResults()  
  }
}