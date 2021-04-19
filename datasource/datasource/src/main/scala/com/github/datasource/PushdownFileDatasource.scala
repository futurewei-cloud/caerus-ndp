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
import com.github.datasource.hdfs.{HdfsFileScan, HdfsStore, PushdownFileFormat}
import org.apache.hadoop.fs.FileStatus
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{SessionConfigSupport, SupportsRead,
                                               Table, TableCapability, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.v2.{FileDataSourceV2, FileScanBuilder, FileTable}
import org.apache.spark.sql.execution.datasources.v2.csv.CSVWriteBuilder
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/** Creates a data source object for Spark that
 *  supports pushdown of predicates such as Project, and Filter.
 *
 */
class PushdownFileDatasource extends FileDataSourceV2
    with SessionConfigSupport  {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[PushdownFileFormat]

  private val logger = LoggerFactory.getLogger(getClass)
  override def toString: String = s"PushdownDataSource()"
  override def supportsExternalMetadata(): Boolean = true

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    throw new IllegalArgumentException("requires a user-supplied schema")
  }
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new IllegalArgumentException("requires a user-supplied schema")
  }
  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val optionsWithoutPaths = getOptionsWithoutPaths(options)
    new PushdownFileTable(sparkSession, optionsWithoutPaths, paths, schema, fallbackFileFormat)
  }

  override def keyPrefix(): String = {
    "pushdownFile"
  }
  override def shortName(): String = "pushdownFile"
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
 case class PushdownFileTable(
    sparkSession: SparkSession,
    options: CaseInsensitiveStringMap,
    paths: Seq[String],
    userDefinedSchema: StructType,
    fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, Some(userDefinedSchema)) {

  private val logger = LoggerFactory.getLogger(getClass)
  // override def fallbackFileFormat: Class[_ <: FileFormat] = fallbackFileFormat
  override def name(): String = this.getClass.toString
  override def formatName(): String = "pushdownFile"
  def inferSchema(files: Seq[org.apache.hadoop.fs.FileStatus]):
      Option[StructType] = Some(schema)

  override def newScanBuilder(params: CaseInsensitiveStringMap): ScanBuilder =
      new PushdownFileScanBuilder(sparkSession, fileIndex, schema, dataSchema, paths,
                                  options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new CSVWriteBuilder(paths, formatName, supportsDataType, info)

  override def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: AtomicType => true

    // case udt: UserDefinedType[_] => true supportsDataType(udt.sqlType)

    case _ => false
  }
}

/** Creates a builder for scan objects.
 *  For hdfs HdfsScan.
 *
 * @param schema the format of the columns
 * @param options the options (see PushdownBatchTable for full list.)
 */
 case class PushdownFileScanBuilder(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    schema: StructType,
    dataSchema: StructType,
    paths: Seq[String],
    options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema)
      with SupportsPushDownRequiredColumns {

  private val logger = LoggerFactory.getLogger(getClass)
  private var prunedSchema: StructType = schema

  /** Returns a scan object for this particular query.
   *   Currently we only support Hdfs.
   *
   * @return the scan object either a HdfsScan
   */
  override def build(): Scan = {
    if (false && !options.get("path").contains("hdfs")) {
      throw new Exception(s"endpoint ${options.get("endpoint")} is unexpected")
    }
    new HdfsFileScan(sparkSession,
      fileIndex,
      dataSchema,
      readDataSchema(),
      readPartitionSchema(),
      paths,
      options)
  }
  private val shouldPushdown: Boolean = {
    HdfsStore.pushdownSupported(options)
  }
  /** returns true if pushdowns are supported for this type of connector.
   *
   * @return true if pushdown supported, false otherwise
   */
  private def pushdownSupported(): Boolean = {
    if (false && !options.get("path").contains("hdfs")) {
      throw new Exception(s"path ${options.get("path")} is unexpected")
    }
    shouldPushdown
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
