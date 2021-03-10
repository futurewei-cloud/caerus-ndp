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
package com.github.datasource

import java.util

import scala.collection.JavaConverters._

import com.github.datasource.common.Pushdown
import com.github.datasource.hdfs.HdfsStore
import org.slf4j.LoggerFactory

import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, SupportsRead,
                                               Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import com.github.datasource.hdfs.HdfsScan

/** Creates a data source object for Spark that
 *  supports pushdown of predicates such as Project, and Filter.
 *
 */
class DefaultSource extends TableProvider
  with SessionConfigSupport with DataSourceRegister {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Pushdown Data Source Created")
  override def toString: String = s"PushdownDataSource()"
  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new IllegalArgumentException("requires a user-supplied schema")
  }

  override def getTable(schema: StructType,
                        transforms: Array[Transform],
                        options: util.Map[String, String]): Table = {
    logger.trace("getTable: Options " + options)
    new PushdownBatchTable(schema, options)
  }

  override def keyPrefix(): String = {
    "pushdown"
  }
  override def shortName(): String = "pushdown"
}

/** Creates a Table object that supports pushdown predicates
 *   such as Filter, Project.
 *
 * @param schema the StructType format of this table
 * @param options the parameters for creating the table
 *                "endpoint" is the server name,
 *                "accessKey" and "secretKey" are the credentials for above server.
 *                 "path" is the full path to the file.
 */
class PushdownBatchTable(schema: StructType,
                         options: util.Map[String, String])
  extends Table with SupportsRead {

  private val logger = LoggerFactory.getLogger(getClass)
  logger.trace("Created")
  override def name(): String = this.getClass.toString

  override def schema(): StructType = schema

  override def capabilities(): util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava

  override def newScanBuilder(params: CaseInsensitiveStringMap): ScanBuilder =
      new PushdownScanBuilder(schema, options)
}

/** Creates a builder for scan objects.
 *  For hdfs HdfsScan.
 *
 * @param schema the format of the columns
 * @param options the options (see PushdownBatchTable for full list.)
 */
class PushdownScanBuilder(schema: StructType,
                          options: util.Map[String, String])
  extends ScanBuilder
    with SupportsPushDownRequiredColumns {

  private val logger = LoggerFactory.getLogger(getClass)
  private var prunedSchema: StructType = schema

  logger.trace("Created")

  /** Returns a scan object for this particular query.
   *   Currently we only support Hdfs.
   *
   * @return the scan object either a HdfsScan
   */
  override def build(): Scan = {    
    if (!options.get("path").contains("hdfs")) {
      throw new Exception(s"endpoint ${options.get("endpoint")} is unexpected")
    }
    new HdfsScan(schema, options, prunedSchema)
  }
  /** returns true if pushdowns are supported for this type of connector.
   *
   * @return true if pushdown supported, false otherwise
   */
  private def pushdownSupported(): Boolean = {    
    if (!options.get("path").contains("hdfs")) {
      throw new Exception(s"path ${options.get("path")} is unexpected")
    }
    HdfsStore.pushdownSupported(options)
  }
  /** Pushes down the list of columns specified by requiredSchema
   *
   * @param requiredSchema the list of coumns we should use, and prune others.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    if (pushdownSupported() && !options.containsKey("DisableProjectPush")) {
      prunedSchema = requiredSchema
      logger.info("pruneColumns " + requiredSchema.toString)
    }
  }
}
