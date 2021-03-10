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

import scala.xml._
import java.io.StringWriter
import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition

/** Is a request or message to be sent to a server to 
 *  query data and other extraneous processing of that data.
 *
 * @param schema the representation of the column data
 * @param query the SQL operation to perform or empty string if none.
 */
class ProcessorRequest(schema: String,
                       query: String,
                       blockSize: Long) {

    def toXml : String = {
        val root = <Processor>
                     <Name>dikeSQL</Name>
                     <Version>0.1 </Version>
                     <Configuration>
                       <Schema>{schema}</Schema>
                       <Query>{scala.xml.PCData(query)}</Query>
                       <BlockSize>{blockSize}</BlockSize>
                       <FieldDelimiter>{','.toInt}</FieldDelimiter>
                       <RowDelimiter>{'\n'.toInt}</RowDelimiter>
                       <QuoteDelimiter>{'"'.toInt}</QuoteDelimiter>
                     </Configuration>
                   </Processor>
        val writer = new StringWriter
        XML.write(writer, root, "UTF-8", true, null)
        writer.flush()
        //println(writer.toString.replace("\n", "").replace("  ", ""))
        writer.toString.replace("\n", "")
    }
}