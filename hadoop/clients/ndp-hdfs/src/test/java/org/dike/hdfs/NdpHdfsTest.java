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
package org.dike.hdfs;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;

import java.io.StringWriter;
import java.io.FileNotFoundException;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.nio.charset.StandardCharsets;

import java.util.Map;

import javax.xml.stream.XMLStreamWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.commons.io.input.BoundedInputStream;

/**
 * Unit test for NdpHdfsFileSystem.
 */
public class NdpHdfsTest
{
    private boolean traceEnabled = false;
    public String GetProjectionConfig(boolean skipHeader, int columns, int [] projection,
                                      long blockSize) throws XMLStreamException
    {
        XMLOutputFactory xmlof = XMLOutputFactory.newInstance();
        StringWriter strw = new StringWriter();
        XMLStreamWriter xmlw = xmlof.createXMLStreamWriter(strw);
        xmlw.writeStartDocument();
        xmlw.writeStartElement("Processor");

        xmlw.writeStartElement("Name");
        xmlw.writeCharacters("Project");
        xmlw.writeEndElement(); // Name
        //xmlw.writeAttribute("Version","0.1");
        xmlw.writeStartElement("Configuration");

        xmlw.writeStartElement("SkipHeader");
        xmlw.writeCharacters(String.valueOf(skipHeader));
        xmlw.writeEndElement(); // SkipHeader

        xmlw.writeStartElement("Columns");
        xmlw.writeCharacters(String.valueOf(columns));
        xmlw.writeEndElement(); // Columns

        xmlw.writeStartElement("Project");
        String projectionString = "";
        for(int i = 0; i < projection.length; i++){
            projectionString += String.valueOf(projection[i]);
            if (i < projection.length - 1) {
                projectionString += ",";
            }
        }
        xmlw.writeCharacters(projectionString);
        xmlw.writeEndElement(); // Project

        xmlw.writeStartElement("BlockSize");
        xmlw.writeCharacters(String.valueOf(blockSize));
        xmlw.writeEndElement(); // BlockSize

        xmlw.writeEndElement(); // Configuration
        xmlw.writeEndElement(); // Processor
        xmlw.writeEndDocument();
        xmlw.close();

        return strw.toString();
    }

    /**
     * Simple NdpHdfs Test :-)
     */

    @Test
    @DisplayName("Simple NdpHdfs test")
    public void testNdpHdfs()
    {
        int[] projection = new int [] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        test("NdpHdfs Sorted", projection);
        projection = new int [] {0, 2, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
        test("NdpHdfs Random", projection);
        test("NdpHdfs NoPushdown", null);
    }

    public void test(String name, int projection[])
    {        
        long totalRecords = 0;
        final Path fname = new Path("/lineitem.csv");
        String hadoopPath = System.getenv("HADOOP_PATH");
        assertTrue( hadoopPath != null );        
        Configuration conf = new Configuration();
        assertTrue( conf != null );
        conf.addResource( new Path(hadoopPath + "/etc/hadoop/core-client.xml") );
        conf.addResource( new Path(hadoopPath + "/etc/hadoop/hdfs-site.xml") );

        //System.out.println(conf.get("fs.defaultFS"));
        Path ndpHdfsPath = new Path("ndphdfs://hadoop-ndp:9870/");
        try {
            FileSystem fs = FileSystem.get(ndpHdfsPath.toUri(), conf);
            BlockLocation[] locs = fs.getFileBlockLocations(fname, 0, Long.MAX_VALUE);
            Map<String,Statistics> stats = fs.getStatistics();
            stats.get(fs.getScheme()).reset();

            long start_time = System.currentTimeMillis();
            //int projection[] = {0, 2, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
            //int projection[] = {0, 1, 15 };
            for (BlockLocation loc : locs) {
                //if (loc.getOffset() > 0){ continue;}
                if(traceEnabled) {
                    System.out.format("Offset=%d Length=%d\n", loc.getOffset(), loc.getLength());
                }
                NdpHdfsFileSystem ndpFS = (NdpHdfsFileSystem)fs;
                FSDataInputStream is = null;
                if (projection != null){
                    String ndpConfig = GetProjectionConfig(true, 16, projection, loc.getLength());                    
                    is = ndpFS.open(fname, 32 << 10, ndpConfig);
                } else {
                    is = ndpFS.open(fname);
                }
                is.seek(loc.getOffset());
                BufferedReader br = null;
                if (projection != null){
                    br = new BufferedReader(new InputStreamReader(is,StandardCharsets.UTF_8), 128 << 10);
                } else {
                    br = new BufferedReader(new InputStreamReader(new BoundedInputStream(is, loc.getLength())));
                }

                String record = br.readLine();
                int counter = 0;
                while (record != null) {
                    if(traceEnabled && counter < 5 /* || counter > 1064140 */) {
                        System.out.println(record);
                    }                        
                    totalRecords += 1;
                    counter += 1;

                    record = br.readLine(); // Should be last !!!
                }
                br.close();
        }
            long end_time = System.currentTimeMillis();            
            System.out.format("%s: Received %d records (%d bytes) in %.3f sec\n", name, totalRecords, 
                stats.get(fs.getScheme()).getBytesRead(), (end_time - start_time) / 1000.0);
        } catch (FileNotFoundException ex){
            System.out.println(ex.getMessage());
            assertTrue( false );
        } catch (Exception ex) {
            ex.printStackTrace();
            assertTrue( false );
        }        
    }
}
