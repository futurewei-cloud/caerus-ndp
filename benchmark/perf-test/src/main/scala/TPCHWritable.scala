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

import org.apache.commons.io.IOUtils
import org.apache.spark.sql.SQLContext
import com.databricks.spark.sql.perf.Benchmark
import com.databricks.spark.sql.perf.ExecutionMode

/** A TPC-H benchmark.  We specifically override this test in order to
 *  control the output execution mode.
 *
 * @param sqlContext - Needed by the base class Benchmark
 * @param executionMode - We added this to give control as to what happens with results.
 *                        For example, we would like to override this to write the output.
 *                        DataFrame for later analysis.
 */
class TPCHWritable(@transient sqlContext: SQLContext,
                   executionMode: ExecutionMode = ExecutionMode.CollectResults)
  extends Benchmark(sqlContext) {

  val queries = (1 to 22).map { q =>
    val queryContent: String = IOUtils.toString(
      getClass().getClassLoader().getResourceAsStream(s"tpch/queries/$q.sql"))
    Query(s"Q$q", queryContent, description = "TPCH Query",
      executionMode = executionMode)
  }
  val queriesMap = queries.map(q => q.name.split("-").get(0) -> q).toMap
}