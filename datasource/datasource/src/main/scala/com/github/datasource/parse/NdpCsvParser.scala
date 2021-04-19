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

import java.io.InputStream

import scala.util.control.NonFatal

import com.univocity.parsers.csv.CsvParser
import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.{InternalRow, NoopFilters, OrderedFilters}
import org.apache.spark.sql.catalyst.csv.CSVOptions
import org.apache.spark.sql.catalyst.expressions.{ExprUtils, GenericInternalRow}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.util.LegacyDateFormats.FAST_DATE_FORMAT
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

object NdpCsvParser {

  def parseIterator(
      lines: Iterator[String],
      parser: NdpUnivocityParser,
      schema: StructType): Iterator[InternalRow] = {

    val options = parser.options

    val safeParser = new FailureSafeParser[String](
      input => parser.parse(input),
      PermissiveMode,
      schema,
      "")
    lines.flatMap(safeParser.parse)
    // lines.map(l => RowIterator.parseLine(l, schema))
  }
  def readFile1(
      name: String, offset: Long, length: Long,
      parser: NdpUnivocityParser,
      requiredSchema: StructType): Iterator[InternalRow] = {
    val lines = {
      val conf = new Configuration()
      val file = PartitionedFile(InternalRow.empty, name, offset, length)
      val linesReader = new HadoopFileLinesReader(file, parser.options.lineSeparatorInRead, conf)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
      linesReader.map { line =>
        new String(line.getBytes, 0, line.getLength, parser.options.charset)
      }
    }
    parseIterator(lines, parser, requiredSchema)
  }
  def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: NdpUnivocityParser,
      requiredSchema: StructType): Iterator[InternalRow] = {
    val lines = {
      val linesReader = new HadoopFileLinesReader(file, parser.options.lineSeparatorInRead, conf)
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
      linesReader.map { line =>
        new String(line.getBytes, 0, line.getLength, parser.options.charset)
      }
    }
    parseIterator(lines, parser, requiredSchema)
  }
}



/**
 * Constructs a parser for a given schema that translates CSV data to an [[InternalRow]].
 *
 * @param dataSchema The CSV data schema that is specified by the user, or inferred from underlying
 *                   data files.
 * @param requiredSchema The schema of the data that should be output for each row. This should be a
 *                       subset of the columns in dataSchema.
 * @param options Configuration options for a CSV parser.
 * @param filters The pushdown filters that should be applied to converted values.
 */
class NdpUnivocityParser(
    dataSchema: StructType,
    requiredSchema: StructType,
    val options: CSVOptions,
    filters: Seq[Filter]) extends Logging {
  require(requiredSchema.toSet.subsetOf(dataSchema.toSet),
    s"requiredSchema (${requiredSchema.catalogString}) should be the subset of " +
      s"dataSchema (${dataSchema.catalogString}).")

  def this(dataSchema: StructType, requiredSchema: StructType, options: CSVOptions) = {
    this(dataSchema, requiredSchema, options, Seq.empty)
  }
  def this(schema: StructType, options: CSVOptions) = this(schema, schema, options)

  // A `ValueConverter` is responsible for converting the given value to a desired type.
  private type ValueConverter = String => Any

  // This index is used to reorder parsed tokens
  private val tokenIndexArr =
    requiredSchema.map(f => java.lang.Integer.valueOf(dataSchema.indexOf(f))).toArray

  // When column pruning is enabled, the parser only parses the required columns based on
  // their positions in the data schema.
  private val parsedSchema = if (options.columnPruning) requiredSchema else dataSchema

  val tokenizer: CsvParser = {
    val parserSetting = options.asParserSettings
    // When to-be-parsed schema is shorter than the to-be-read data schema, we let Univocity CSV
    // parser select a sequence of fields for reading by their positions.
    if (parsedSchema.length < dataSchema.length) {
      parserSetting.selectIndexes(tokenIndexArr: _*)
    }
    new CsvParser(parserSetting)
  }

  // Pre-allocated Some to avoid the overhead of building Some per each-row.
  private val requiredRow = Some(new GenericInternalRow(requiredSchema.length))
  // Pre-allocated empty sequence returned when the parsed row cannot pass filters.
  // We preallocate it avoid unnecessary allocations.
  private val noRows = None

  private lazy val timestampFormatter = TimestampFormatter(
    options.timestampFormat,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)
  private lazy val dateFormatter = DateFormatter(
    options.dateFormat,
    options.zoneId,
    options.locale,
    legacyFormat = FAST_DATE_FORMAT,
    isParsing = true)

  private val csvFilters = if (SQLConf.get.csvFilterPushDown) {
    new OrderedFilters(filters, requiredSchema)
  } else {
    new NoopFilters
  }

  // Retrieve the raw record string.
  private def getCurrentInput: UTF8String = {
    val currentContent = tokenizer.getContext.currentParsedContent()
    if (currentContent == null) null else UTF8String.fromString(currentContent.stripLineEnd)
  }

  // This parser first picks some tokens from the input tokens, according to the required schema,
  // then parse these tokens and put the values in a row, with the order specified by the required
  // schema.
  //
  // For example, let's say there is CSV data as below:
  //
  //   a,b,c
  //   1,2,A
  //
  // So the CSV data schema is: ["a", "b", "c"]
  // And let's say the required schema is: ["c", "b"]
  //
  // with the input tokens,
  //
  //   input tokens - [1, 2, "A"]
  //
  // Each input token is placed in each output row's position by mapping these. In this case,
  //
  //   output row - ["A", 2]
  private val valueConverters: Array[ValueConverter] = {
    requiredSchema.map(f => makeConverter(f.name, f.dataType, f.nullable)).toArray
  }

  private val decimalParser = ExprUtils.getDecimalParser(options.locale)

  /**
   * Create a converter which converts the string value to a value according to a desired type.
   * Currently, we do not support complex types (`ArrayType`, `MapType`, `StructType`).
   *
   * For other nullable types, returns null if it is null or equals to the value specified
   * in `nullValue` option.
   */
  def makeConverter(
      name: String,
      dataType: DataType,
      nullable: Boolean = true): ValueConverter = dataType match {
    case _: ByteType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toByte)

    case _: ShortType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toShort)

    case _: IntegerType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toInt)

    case _: LongType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toLong)

    case _: FloatType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) {
        case options.nanValue => Float.NaN
        case options.negativeInf => Float.NegativeInfinity
        case options.positiveInf => Float.PositiveInfinity
        case datum => datum.toFloat
      }

    case _: DoubleType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) {
        case options.nanValue => Double.NaN
        case options.negativeInf => Double.NegativeInfinity
        case options.positiveInf => Double.PositiveInfinity
        case datum => datum.toDouble
      }

    case _: BooleanType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(_.toBoolean)

    case dt: DecimalType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        Decimal(decimalParser(datum), dt.precision, dt.scale)
      }

    case _: TimestampType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        try {
          timestampFormatter.parse(datum)
        } catch {
          case NonFatal(e) =>
            // If fails to parse, then tries the way used in 2.0 and 1.x for backwards
            // compatibility.
            val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(datum))
            DateTimeUtils.stringToTimestamp(str, options.zoneId).getOrElse(throw e)
        }
      }

    case _: DateType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        try {
          dateFormatter.parse(datum)
        } catch {
          case NonFatal(e) =>
            // If fails to parse, then tries the way used in 2.0 and 1.x for backwards
            // compatibility.
            val str = DateTimeUtils.cleanLegacyTimestampStr(UTF8String.fromString(datum))
            DateTimeUtils.stringToDate(str, options.zoneId).getOrElse(throw e)
        }
      }

    case _: StringType => (d: String) =>
      nullSafeDatum(d, name, nullable, options)(UTF8String.fromString)

    case CalendarIntervalType => (d: String) =>
      nullSafeDatum(d, name, nullable, options) { datum =>
        IntervalUtils.safeStringToInterval(UTF8String.fromString(datum))
      }

    // case udt: UserDefinedType[_] =>
    //   makeConverter(name, udt.sqlType, nullable)

    // We don't actually hit this exception though, we keep it for understandability
    case _ => throw new RuntimeException(s"Unsupported type: ${dataType.typeName}")
  }

  private def nullSafeDatum(
       datum: String,
       name: String,
       nullable: Boolean,
       options: CSVOptions)(converter: ValueConverter): Any = {
    if (datum == options.nullValue || datum == null) {
      if (!nullable) {
        throw new RuntimeException(s"null value found but field $name is not nullable.")
      }
      null
    } else {
      converter.apply(datum)
    }
  }

  /**
   * Parses a single CSV string and turns it into either one resulting row or no row (if the
   * the record is malformed).
   */
  val parse: String => Option[InternalRow] = {
    // This is intentionally a val to create a function once and reuse.
    if (options.columnPruning && requiredSchema.isEmpty) {
      // If `columnPruning` enabled and partition attributes scanned only,
      // `schema` gets empty.
      (_: String) => Some(InternalRow.empty)
    } else {
      // parse if the columnPruning is disabled or requiredSchema is nonEmpty
      (input: String) => convert(tokenizer.parseLine(input))
    }
  }


  private val getToken = if (options.columnPruning) {
    (tokens: Array[String], index: Int) => tokens(index)
  } else {
    (tokens: Array[String], index: Int) => tokens(tokenIndexArr(index))
  }

  private def convert(tokens: Array[String]): Option[InternalRow] = {
    if (tokens == null) {
      throw BadRecordException(
        () => getCurrentInput,
        () => None,
        new RuntimeException("Malformed CSV record"))
    }

    var badRecordException: Option[Throwable] = if (tokens.length != parsedSchema.length) {
      // If the number of tokens doesn't match the schema, we should treat it as a malformed record.
      // However, we still have chance to parse some of the tokens. It continues to parses the
      // tokens normally and sets null when `ArrayIndexOutOfBoundsException` occurs for missing
      // tokens.
      Some(new RuntimeException("Malformed CSV record"))
    } else None
    // When the length of the returned tokens is identical to the length of the parsed schema,
    // we just need to:
    //  1. Convert the tokens that correspond to the required schema.
    //  2. Apply the pushdown filters to `requiredRow`.
    var i = 0
    val row = requiredRow.get
    var skipRow = false
    while (i < requiredSchema.length) {
      try {
        if (skipRow) {
          row.setNullAt(i)
        } else {
          row(i) = valueConverters(i).apply(getToken(tokens, i))
          if (csvFilters.skipRow(row, i)) {
            skipRow = true
          }
        }
      } catch {
        // case e: SparkUpgradeException => throw e
        case NonFatal(e) =>
          badRecordException = badRecordException.orElse(Some(e))
          row.setNullAt(i)
      }
      i += 1
    }
    if (skipRow) {
      noRows
    } else {
      if (badRecordException.isDefined) {
        throw BadRecordException(
          () => getCurrentInput, () => requiredRow.headOption, badRecordException.get)
      } else {
        requiredRow
      }
    }
  }
}

object NdpUnivocityParser {

    /**
   * Parses an iterator that contains CSV strings and turns it into an iterator of rows.
   */
  def parseIterator(
      lines: Iterator[String],
      parser: NdpUnivocityParser,
      schema: StructType): Iterator[InternalRow] = {

    val options = parser.options

    val filteredLines: Iterator[String] = lines
      // CSVExprUtils.filterCommentAndEmpty(lines, options)

    val safeParser = new FailureSafeParser[String](
      input => parser.parse(input),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)
    filteredLines.flatMap(safeParser.parse)
  }
}
