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
package com.github.datasource.hdfs

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer}

import com.github.datasource.parse.{NdpCsvParser, NdpUnivocityParser}
import org.apache.hadoop.fs.BlockLocation
import org.slf4j.LoggerFactory

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.csv.{CSVHeaderChecker, CSVOptions}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprUtils}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.csv.CSVDataSource
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.datasources.v2.FileScan
import org.apache.spark.sql.execution.datasources.v2.PartitionReaderFromIterator
import org.apache.spark.sql.execution.datasources.v2.PartitionReaderWithPartitionValues
import org.apache.spark.sql.execution.datasources.v2.TextBasedFileScan
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration

/** A scan object that works on HDFS files.
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param prunedSchema the new array of columns after pruning
 */
 case class HdfsFileScan(
    sparkSession: SparkSession,
    fileIndex: PartitioningAwareFileIndex,
    dataSchema: StructType,
    readDataSchema: StructType,
    readPartitionSchema: StructType,
    paths: Seq[String],
    options: CaseInsensitiveStringMap,
    partitionFilters: Seq[Expression] = Seq.empty,
    dataFilters: Seq[Expression] = Seq.empty)
  extends TextBasedFileScan(sparkSession, options) {

  private var schema = dataSchema
  private var prunedSchema = readDataSchema
  private val logger = LoggerFactory.getLogger(getClass)
  private val maxPartSize: Long = (1024 * 1024 * 128)
  private var filePartitions: Array[InputPartition] = getPartitions()

  override def withFilters(
      partitionFilters: Seq[Expression], dataFilters: Seq[Expression]): FileScan =
    this.copy(partitionFilters = partitionFilters, dataFilters = dataFilters)

  private def createPartitions(blockMap: Map[String, Array[BlockLocation]],
                               store: HdfsStore): Array[InputPartition] = {
    var a = new ArrayBuffer[InputPartition](0)
    var i = 0
    if (options.containsKey("partitions") &&
          options.get("partitions").toInt == 1) {
      // Generate one partition per file
      for ((fName, blockList) <- blockMap) {
        a += new HdfsPartition(index = 0, offset = 0, length = store.getLength(fName),
                               name = fName)
      }
    } else {
      // Generate one partition per file, per hdfs block
      for ((fName, blockList) <- blockMap) {
        // Generate one partition per hdfs block.
        for (block <- blockList) {
          a += new HdfsPartition(index = i, offset = block.getOffset, length = block.getLength,
                                 name = fName)
          i += 1
        }
      }
    }
    logger.info(a.toArray.mkString(", "))
    a.toArray
  }
  /** Returns an Array of S3Partitions for a given input file.
   *  the file is selected by options("path").
   *  If there is one file, then we will generate multiple partitions
   *  on that file if large enough.
   *  Otherwise we generate one partition per file based partition.
   *
   * @return array of S3Partitions
   */
  private def getPartitions(): Array[InputPartition] = {
    var store: HdfsStore = HdfsStoreFactory.getStore(schema, paths, options,
                                                     prunedSchema)
    val fileName = store.filePath
    val blocks : Map[String, Array[BlockLocation]] = store.getBlockList(fileName)
    createPartitions(blocks, store)
  }

  // override def planInputPartitions(): Array[InputPartition] = {
  //  filePartitions
  // }
  override def createReaderFactory(): PartitionReaderFactory = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))
    // The partition values are already truncated in `FileScan.partitions`.
    // We should use `readPartitionSchema` as the partition schema here.
    new HdfsFilePartitionReaderFactory(broadcastedConf,
      dataSchema, readDataSchema, readPartitionSchema, paths, options.asCaseSensitiveMap)
  }
}

/** Creates a factory for creating HdfsPartitionReader objects
 *
 * @param schema the column format
 * @param options the options including "path"
 * @param prunedSchema the new array of columns after pruning
 */
class HdfsFilePartitionReaderFactory(
    broadcastedConf: Broadcast[SerializableConfiguration],
    dataSchema: StructType,
    readDataSchema: StructType,
    partitionSchema: StructType,
    paths: Seq[String],
    options: java.util.Map[String, String],
    filters: Seq[Filter] = Seq.empty[Filter]) extends FilePartitionReaderFactory {
  private val logger = LoggerFactory.getLogger(getClass)

  /* override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    var store: HdfsStore = HdfsStoreFactory.getStore(dataSchema, paths, options,
                                                     readDataSchema)
    new PartitionReaderFromIterator(store.getRowIter(partition.asInstanceOf[HdfsPartition]))
  } */

  def buildReaderOrig(file: PartitionedFile): PartitionReader[InternalRow] = {
    val csvOptions = new CSVOptions(
    scala.collection.immutable.Map.empty[String, String], false, "UTC")
    val conf = broadcastedConf.value.value
    val parser = new NdpUnivocityParser(
      dataSchema,
      readDataSchema,
      csvOptions,
      filters)
   val iter = NdpCsvParser.readFile(
      conf,
      file,
      parser,
      readDataSchema)
    val fileReader = new PartitionReaderFromIterator[InternalRow](iter)
    new PartitionReaderWithPartitionValues(fileReader, readDataSchema,
      partitionSchema, file.partitionValues)
  }

  override def buildReader(file: PartitionedFile): PartitionReader[InternalRow] = {
    val csvOptions = new CSVOptions(
    scala.collection.immutable.Map.empty[String, String], false, "UTC")
    val conf = broadcastedConf.value.value
    val parser = new NdpUnivocityParser(
      dataSchema,
      readDataSchema,
      csvOptions,
      filters)
   val iter = NdpCsvParser.readFile(
      conf,
      file,
      parser,
      readDataSchema)
    val readFunction = buildReaderWithPartitionValues(iter, readDataSchema)
    val iter2 = readFunction(file)
    new PartitionReaderFromIterator[InternalRow](iter2)
  }

  def buildReaderWithPartitionValues(
      dataReader: Iterator[InternalRow],
      requiredSchema: StructType): PartitionedFile => Iterator[InternalRow] = {

    new (PartitionedFile => Iterator[InternalRow]) with Serializable {
      private val fullSchema = requiredSchema.map(
        f => AttributeReference(f.name, f.dataType, f.nullable, f.metadata)())
      // requiredSchema.toAttributes

      // Using lazy val to avoid serialization
      private lazy val appendPartitionColumns =
        GenerateUnsafeProjection.generate(fullSchema, fullSchema)

      override def apply(file: PartitionedFile): Iterator[InternalRow] = {
        // Using local val to avoid per-row lazy val check (pre-mature optimization?...)
        val converter = appendPartitionColumns

        // Note that we have to apply the converter even though `file.partitionValues` is empty.
        // This is because the converter is also responsible for converting safe `InternalRow`s into
        // `UnsafeRow`s.
        // dataReader(file).map
        dataReader.map { dataRow =>
          converter(dataRow)
        }
      }
    }
  }
}
