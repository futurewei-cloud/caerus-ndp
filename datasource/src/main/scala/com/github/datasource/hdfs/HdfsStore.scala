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

import java.io.BufferedReader
import java.io.InputStream
import java.io.InputStreamReader
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map}
import scala.util.control.NonFatal

import com.github.datasource.common.Pushdown
import com.github.datasource.parse.RowIteratorFactory
import org.apache.commons.io.IOUtils
import org.apache.commons.io.input.BoundedInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.BlockLocation
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.web.TokenAspect
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem
import org.dike.hdfs.NdpHdfsFileSystem
import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._

/** A Factory to fetch the correct type of
 *  store object.
 */
object HdfsStoreFactory {
  /** Returns the store object.
   *
   * @param schema the StructType schema to construct store with.
   * @param params the parameters including those to construct the store
   * @param prunedSchema the schema to pushdown (column pruning)
   * @return a new HdfsStore object constructed with above parameters.
   */
  def getStore(schema: StructType,
               params: java.util.Map[String, String],
               prunedSchema: StructType): HdfsStore = {
    new HdfsStore(schema, params, prunedSchema)
  }
}
/** A hdfs store object which can connect
 *  to a file on hdfs filesystem, specified by params("path"),
 *  And which can read a partition with any of various pushdowns.
 *
 * @param schema the StructType schema to construct store with.
 * @param params the parameters including those to construct the store
 * @param prunedSchema the schema to pushdown (column pruning)
 */
class HdfsStore(schema: StructType,
                params: java.util.Map[String, String],
                prunedSchema: StructType) {

  override def toString() : String = "HdfsStore" + params
  protected val path = params.get("path")
  protected val fileFormat = {
    if (params.containsKey("format")) {
      params.get("format")
    } else {
      // Default is csv if it is not selected.
      "csv"
    }
  }
  protected val isPushdownNeeded: Boolean = {
    /* Determines if we should send the pushdown to ndp.
     * If any of the pushdowns are in use (project),
     * then we will consider that pushdown is needed.
     */
    (prunedSchema.length != schema.length)
  }
  protected val endpoint = {
    val server = path.split("/")(2)
    if (path.contains("ndphdfs://")) {
      ("ndphdfs://" + server + ":9870")
    } else if (path.contains("webhdfs://")) {
      ("webhdfs://" + server + ":9870")
    } else {
      ("hdfs://" + server + ":9000")
    }
  }
  protected val logger = LoggerFactory.getLogger(getClass)
  protected val (readColumns: String,
                 readSchema: StructType) = {
    var columns = Pushdown.getColumnSchema(prunedSchema)
    (columns, schema)
  }
  protected val fileSystem = {
    val conf = new Configuration()
    // Configure to pick up our NDP Filesystem.
    conf.set("fs.ndphdfs.impl", classOf[org.dike.hdfs.NdpHdfsFileSystem].getName)

    if (path.contains("ndphdfs")) {
      val fs = FileSystem.get(URI.create(endpoint), conf)
      fs.asInstanceOf[NdpHdfsFileSystem]
    } else {
      FileSystem.get(URI.create(endpoint), conf)
    }
  }
  val filePath = {
    val server = path.split("/")(2)
    var pathName = {
      if (path.contains("ndphdfs://")) {
        val str = path.replace("ndphdfs://" + server, "ndphdfs://" + server + ":9870")
        str
      } else if (path.contains("webhdfs")) {
        path.replace(server, server + ":9870")
      } else {
        path.replace(server, server + ":9000")
      }
    }
    pathName
  }
  protected val fileSystemType = fileSystem.getScheme
  /** returns the index of a field matching an input name in a schema.
   *
   * @param schema the schema to scan
   * @param name the name of the field to search for in schema
   *
   * @return Integer - The index of this field in the input schema.
   */
  def getSchemaIndex(schema: StructType, name: String): Integer = {
    for (i <- 0 to schema.fields.size) {
      if (schema.fields(i).name == name) {
        return i
      }
    }
    -1
  }
  /** Returns a reader for a given Hdfs partition.
   *  Determines the correct start offset by looking backwards
   *  to find the end of the prior line.
   *  Helps in cases where the last line of the prior partition
   *  was split on the partition boundary.  In that case, the
   *  prior partition's last (incomplete) is included in the next partition.
   *
   * @param partition the partition to read
   * @return a new BufferedReader for this partition.
   */
  def getReader(partition: HdfsPartition,
                startOffset: Long = 0, length: Long = 0): BufferedReader = {
    val filePath = new Path(partition.name)
    val readParam = {
      if (!isPushdownNeeded ||
          fileSystemType != "ndphdfs") {
        ""
      } else {
        val projectColumns = prunedSchema.fields.map(x => {
            getSchemaIndex(schema, x.name)
        }).mkString(",")
        new ProcessorRequest(schema.fields.size.toString, projectColumns, partition.length).toXml
      }
    }
    /* When we are targeting ndphdfs, but we do not have a pushdown,
     * we will not pass the processor element.
     * This allows the NDP server to optimize further.
     */
    if (isPushdownNeeded &&
        fileSystemType == "ndphdfs") {
        val fs = fileSystem.asInstanceOf[NdpHdfsFileSystem]
        val inStrm = fs.open(filePath, 4096, readParam).asInstanceOf[FSDataInputStream]
        inStrm.seek(partition.offset)
        new BufferedReader(new InputStreamReader(inStrm))
    } else {
        val inStrm = fileSystem.open(filePath)
        if (fileSystemType == "ndphdfs") {
          logger.info(s"No Pushdown to ${fileSystemType} partition: ${partition.toString}")
        }
        inStrm.seek(startOffset)
        new BufferedReader(new InputStreamReader(new BoundedInputStream(inStrm, length)))
    }
  }
  /** Returns a list of BlockLocation object representing
   *  all the hdfs blocks in a file.
   *
   * @param fileName the full filename path
   * @return Map[String, BlockLocation] The Key is the filename
   *                     the value is the Array of BlockLocation
   */
  def getBlockList(fileName: String):
    scala.collection.immutable.Map[String, Array[BlockLocation]] = {

    val fileToRead = new Path(fileName)
    val blockMap = scala.collection.mutable.Map[String, Array[BlockLocation]]()
    if (fileSystem.isFile(fileToRead)) {
      // Use MaxValue to indicate we want info on all blocks.
      blockMap(fileName) = fileSystem.getFileBlockLocations(fileToRead, 0, Long.MaxValue)
    } else {
      /* fileToRead is a directory. So get the contents of this directory.
       * For each file in the directory create a new map entry with filename as key.
       */
      val status = fileSystem.listStatus(fileToRead)
      for (item <- status) {
        if (item.isFile && item.getPath.getName.contains(".csv")) {
          val currentFile = item.getPath.toString
          // Use MaxValue to indicate we want info on all blocks.
          blockMap(currentFile) = fileSystem.getFileBlockLocations(item.getPath, 0, Long.MaxValue)
        }
      }
    }
    blockMap.toMap
  }
  /** Returns the length of the file in bytes.
   *
   * @param fileName the full path of the file
   * @return byte length of the file.
   */
  def getLength(fileName: String) : Long = {
    val fileToRead = new Path(filePath)
    val fileStatus = fileSystem.getFileStatus(fileToRead)
    fileStatus.getLen
  }
  /** Returns the offset, length in bytes of an hdfs partition.
   *  This takes into account any prior lines that might be incomplete
   *  from the prior partition.
   *
   * @param partition the partition to find start for
   * @return (offset, length) - Offset to begin reading partition, Length of partition.
   */
  @throws(classOf[Exception])
  def getPartitionInfo(partition: HdfsPartition) : (Long, Long) = {
    if (fileSystemType == "ndphdfs" && isPushdownNeeded) {
      // No need to find offset, ndp server does this under the covers for us.
      // When Processor is disabled, we need to deal with partial lines for ourselves.
      return (partition.offset, partition.length)
    }
    val currentPath = new Path(partition.name)
    var startOffset = partition.offset
    var nextChar: Integer = 0
    if (partition.offset != 0) {
      /* Scan until we hit a newline. This skips the (normally) partial line,
       * which the prior partition will read, and guarantees we get a full line.
       * The only way to guarantee full lines is by reading up to the line terminator.
       */
      val inputStream = fileSystem.open(currentPath)
      inputStream.seek(partition.offset)
      val reader = new BufferedReader(new InputStreamReader(inputStream))
      do {
        nextChar = reader.read
        startOffset += 1
      } while ((nextChar.toChar != '\n') && (nextChar != -1));
    }
    var partitionLength = (partition.offset + partition.length) - startOffset
    /* Scan up to the next line after the end of the partition.
     * We always include this next line to ensure we are reading full lines.
     * The only way to guarantee full lines is by reading up to the line terminator.
     */
    val inputStream = fileSystem.open(currentPath)
    inputStream.seek(partition.offset + partition.length)
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    do {
      nextChar = reader.read
      // Only count the char if we are not at end of line.
      if (nextChar != -1) {
        partitionLength += 1
      }
    } while ((nextChar.toChar != '\n') && (nextChar != -1));
    (startOffset, partitionLength)
  }
  /** Returns an Iterator over InternalRow for a given Hdfs partition.
   *
   * @param partition the partition to read
   * @return a new CsvRowIterator for this partition.
   */
  def getRowIter(partition: HdfsPartition): Iterator[InternalRow] = {
    val (offset, length) = getPartitionInfo(partition)
    RowIteratorFactory.getIterator(getReader(partition, offset, length),
                                   prunedSchema,
                                   fileFormat)
  }
}

/** Related routines for the HDFS connector.
 *
 */
object HdfsStore {

  /** Returns true if pushdown is supported by this flavor of
   *  filesystem represented by a string of "filesystem://filename".
   *
   * @param options map containing "path".
   * @return true if pushdown supported, false otherwise.
   */
  def pushdownSupported(options: util.Map[String, String]): Boolean = {
    if (options.get("path").contains("ndphdfs://")) {
      true
    } else {
      // other filesystems like hdfs and webhdfs do not support pushdown.
      false
    }
  }
}
