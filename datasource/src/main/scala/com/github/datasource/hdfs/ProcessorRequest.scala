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

import java.io.StringWriter

import scala.xml._

import org.apache.spark.Partition
import org.apache.spark.sql.connector.read.InputPartition

/** Is a request or message to be sent to a server to
 *  perform the peroject operation to prune columns.
 *
 * @param columns number of columns
 * @param project comma separated list of numbers
 * @param query the SQL operation to perform or empty string if none.
 */
class ProcessorRequest(columns: String,
                       project: String,
                       blockSize: Long) {

    def toXml : String = {
        val root = <Processor>
                     <Name>Project</Name>
                     <Configuration>
                       <SkipHeader>false</SkipHeader>
                       <Columns>{columns}</Columns>
                       <Project>{project}</Project>
                       <BlockSize>{blockSize}</BlockSize>
                     </Configuration>
                   </Processor>
        val writer = new StringWriter
        XML.write(writer, root, "UTF-8", true, null)
        writer.flush()
        // println(writer.toString.replace("\n", "").replace("  ", ""))
        writer.toString.replace("\n", "")
    }
}
