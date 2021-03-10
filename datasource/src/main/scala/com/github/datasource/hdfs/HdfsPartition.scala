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

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition
import com.github.datasource.common.PushdownPartition
/** Represents a partition on an hdfs file system
 *
 * @param index the position in order of partitions.
 * @param offset the byte offset from start of file
 * @param length the total bytes in the file
 * @param name the full path of the file
 */
class HdfsPartition(var index: Int,
                    var offset: Long = 0,
                    var length: Long = 0,
                    var name: String = "")
  extends Partition with InputPartition with PushdownPartition {

  override def toString() : String = {
    s"""HdfsPartition index ${index} offset: ${offset} length: ${length} """ +
    s"""name: ${name}"""
  }

  override def preferredLocations(): Array[String] = {
    Array("localhost")
  }
  /** Returns the query clause needed to target this specific partition.
   *
   *  @param partition the S3Partition that is being targeted.
   *
   *  @return String the query clause for use on this partition.
   */
  override def getObjectClause(partition: PushdownPartition): String = {
    val part = partition.asInstanceOf[HdfsPartition]
    "S3Object"
  }
}