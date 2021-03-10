// scalastyle:off
/*
 * Copyright 2018 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Note that portions of this code came from spark-select code:
 *  https://github.com/minio/spark-select/blob/master/src/main/scala/io/minio/spark/select/FilterPushdown.scala
 * 
 * Other portions of this code, most notably compileAggregates, and getColumnSchema,
 * came from this patch by Huaxin Gao:
 *   https://github.com/apache/spark/pull/29695
 */
// scalastyle:on
package com.github.datasource.common

import java.sql.{Date, Timestamp}
import java.util.{Locale, StringTokenizer}

import scala.collection.mutable.ArrayBuilder

import org.slf4j.LoggerFactory

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._

/**
 * Helper methods for pushing filters into Select queries.
 */
object Pushdown {

  protected val logger = LoggerFactory.getLogger(getClass)

  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  def getColumnSchema(schema: StructType):
                     (String, StructType) = {

    val sb = new StringBuilder()
    val columnNames = schema.map(_.name).toArray
    var updatedSchema: StructType = new StructType()
    updatedSchema = schema
    columnNames.foreach(x => sb.append(",").append(x))
    (if (sb.length == 0) "" else sb.substring(1),
     if (sb.length == 0) schema else updatedSchema)
  }

  /** Returns a string to represent the input query.
   *
   * @return String representing the query to send to the endpoint.
   */
  def queryFromSchema(schema: StructType,
                      prunedSchema: StructType,
                      columns: String,
                      partition: PushdownPartition): String = {
    var columnList = prunedSchema.fields.map(x => s"s." + s""""${x.name}"""").mkString(",")

    if (columns.length > 0) {
      columnList = columns
    } else if (columnList.length == 0) {
      columnList = "*"
    }
    val objectClause = partition.getObjectClause(partition)
    var retVal = ""
    retVal = s"SELECT $columnList FROM $objectClause s "
    logger.info(s"SQL Query partition: ${partition.toString}")
    logger.info(s"SQL Query: ${retVal}")
    retVal
  }
  /** Returns a string to represent the schema of the table.
   *
   * @param schema the StructType representing the definition of columns.
   * @return String representing the table's columns.
   */
  def schemaString(schema: StructType): String = {
    
    schema.fields.map(x => {
      val dataTypeString = {
        x.dataType match {
        case IntegerType => "INTEGER"
        case LongType => "LONG"
        case DoubleType => "NUMERIC"
        case _ => "STRING"
        }
      }
      s"${x.name} ${dataTypeString}"
    }).mkString(", ")
  }
}
