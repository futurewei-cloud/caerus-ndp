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
package com.github.datasource.parse

import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.FileWriter
import java.io.File
import java.io.IOException
import java.util.Locale
import java.util

import scala.collection.JavaConverters._

import org.slf4j.LoggerFactory
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.Row
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import com.github.datasource.common.TypeCast

class Delimiters(var fieldDelim: Char,
                 var lineDelim: Char = '\n',
                 var quoteDelim: Char = '\"')

/** A Factory to fetch the correct type of
 *  row Iterator object depending on the file format.
 *
 * RowIteratorFactory.getIterator(rowReader, schema, params)
 */
object RowIteratorFactory {
  /** Returns the Iterator object which can process
   *  the input format from params("format").
   *  Currently only csv and tbl is supported.
   *
   * @param schema the StructType schema to construct store with.
   * @param params the parameters including those to construct the store
   * @param rowReader the BufferedReader that has the
   * @return a new Iterator of InternalRow constructed with above parameters.
   */
  def getIterator(rowReader: BufferedReader,
                  schema: StructType,
                  format: String): Iterator[InternalRow] = {
    format.toLowerCase(Locale.ROOT) match {
      case "csv" => new RowIterator(rowReader, schema, new Delimiters(','))
      case "tbl" => new RowIterator(rowReader, schema, new Delimiters('|'))
    }
  }
}

/** Iterator object that allows for parsing
 *  tbl rows into InternalRow structures.
 *
 * @param rowReader the bufferedReader to fetch data
 * @param schema the format of this stream of data
 */
class RowIterator(rowReader: BufferedReader,
                  schema: StructType,
                  delim: Delimiters)
  extends Iterator[InternalRow] {
  private val logger = LoggerFactory.getLogger(getClass)
  /** Returns an InternalRow parsed from the input line.
   *
   * @param line the String of line to parse
   * @return the InternalRow of this line..
   */
  private def parseLine(line: String): InternalRow = {
    var row = new Array[Any](schema.fields.length)
    var index = 0

    if (delim.fieldDelim != '|') {
      var value: String = ""
      var fieldStart = 0
      while (index < schema.fields.length && fieldStart < line.length) {
        if (line(fieldStart) != delim.quoteDelim) {
          var fieldEnd = line.substring(fieldStart).indexOf(delim.fieldDelim)
          if (fieldEnd == -1) {
            // field is from here to the end of the line
            value = line.substring(fieldStart)
            // Next field start is after comma
            fieldStart = line.length
          } else {
            // field is from start (no skipping) to just before ,
            value = line.substring(fieldStart, fieldStart + fieldEnd)
            // Next field start is after comma
            fieldStart = fieldStart + fieldEnd + 1
          }
        } else {
          // Search from +1 (after ") to next quote
          var fieldEnd = line.substring(fieldStart + 1).indexOf(delim.quoteDelim)
          // Field range is from after " to just before (-1) next quote
          value = line.substring(fieldStart + 1, fieldStart + fieldEnd + 1)
          // Next field start is after quote and comma
          fieldStart = fieldStart + 1 + fieldEnd + 2
        }
        val field = schema.fields(index)
        row(index) = TypeCast.castTo(value, field.dataType, true,
                                    field.nullable)
        index += 1
      }
    } else {
      val values = line.split(delim.fieldDelim)
      if (values.length > 0 && values(0) != "") {
        for (value <- values) {
          val field = schema.fields(index)
          row(index) = TypeCast.castTo(value, field.dataType, true,
            field.nullable)
          index += 1
        }
      }
    }
    if (index >= schema.fields.length) {
      if ((index % 1000) == 0) println(row.toString)
      rows += 1
      new GenericInternalRow(row)
    } else {
      /* If empty, we will simply discard the row since
       * the next partition will pick up this row.
       * This can be expected for some protocols, thus there is no tracing by default.
       */
      //println(s"line too short rows:${rows} received fields:${index}/${schema.fields.length}: ${line}")
      InternalRow.empty
    }
  }
  /** Returns the next row or if none, InternalRow.empty.
   *
   * @return InternalRow for the next row.
   */
  private var nextRow: InternalRow = {
    val firstRow = getNextRow()
    firstRow
  }
  /** Returns row following the current one, 
   *  (if availble), by parsing the next line.
   *
   * @return the next InternalRow object or InternalRow.empty if none.
   */
  private var rows: Long = 0
  private var lastRow: InternalRow = InternalRow.empty
  private def getNextRow(): InternalRow = {
    var line: String = null
    if ({line = rowReader.readLine(); (line == null)}) {
      InternalRow.empty
    } else {
      parseLine(line)
    }
  }
  /** Returns true if there are remaining rows.
   *
   * @return true if rows remaining, false otherwise.
   */
  override def hasNext: Boolean = {
    nextRow.numFields > 0
  }
  /** Returns the following InternalRow
   *
   * @return the next InternalRow or InternalRow.empty if none.
   */
  override def next: InternalRow = {
    val row = nextRow
    nextRow = getNextRow()
    row
  }
  def close(): Unit = {}
}
