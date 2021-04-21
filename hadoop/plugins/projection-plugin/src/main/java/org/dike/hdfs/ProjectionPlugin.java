/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.web;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.List;
import java.util.Arrays;
import java.io.StringReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import static io.netty.buffer.Unpooled.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.CharsetUtil;
import io.netty.util.ByteProcessor;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.LastHttpContent;

import io.netty.handler.codec.http.HttpContentEncoder;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.codec.MessageToMessageCodec;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Values.CLOSE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.parsers.*;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import org.apache.hadoop.conf.Configuration;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

@Sharable
final class ProjectionPlugin 
    extends MessageToMessageCodec<Object, Object> {

  static final Logger REQLOG = LoggerFactory.getLogger("datanode.webhdfs.ProjectionPlugin");
  private Configuration conf;  

  ProjectionPlugin(Configuration conf) {    
    this.conf = conf;    
  }

  public static Configuration initializeState(Configuration conf) {
    System.out.println("ProjectionPlugin::initializeState");
    return(new Configuration(conf));
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    if (msg instanceof HttpRequest) {
      HttpRequest req = (HttpRequest)msg;      
      String ndpConfig = req.headers().get("NdpConfig");      
      if (ndpConfig == null){
        out.add(ReferenceCountUtil.retain(msg));
        return;
      }

      REQLOG.info("ProjectionPlugin : " + req.getUri() + ":" + ndpConfig);

      long offset = 0L;
      List<NameValuePair> params = URLEncodedUtils.parse(new URI(req.uri()), "UTF-8");
      for (NameValuePair param : params) {
        if (param.getName().equals("offset")) {
          offset = Long.parseLong(param.getValue());
        }
      }
      try {
        Parser parser = new Parser(ndpConfig, offset);
        ctx.channel().attr(AttributeKey.valueOf("NdpParser")).set(parser);
        ctx.channel().attr(AttributeKey.valueOf("FlushInProgress")).set(false);
      } catch ( Exception e ) {
        DefaultHttpResponse resp = new DefaultHttpResponse(HTTP_1_1, BAD_REQUEST);
        resp.headers().set(CONNECTION, CLOSE);
        ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
        return;
      }
    }
    out.add(ReferenceCountUtil.retain(msg));
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    Parser parser = (Parser) ctx.channel().attr(AttributeKey.valueOf("NdpParser")).get();

    if (msg instanceof ByteBuf && parser != null) {      
      if (parser.is_done()) {        
        Boolean flushInProgress = (Boolean) ctx.channel().attr(AttributeKey.valueOf("FlushInProgress")).get();
        if(!flushInProgress){          
          ctx.channel().attr(AttributeKey.valueOf("FlushInProgress")).set(true);
          ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }        
        out.add(Unpooled.EMPTY_BUFFER);        
        return;
      }

      ByteBuf outByteBuf = null;
      if (parser.isSorted) {
        outByteBuf = parser.parseSorted((ByteBuf)msg);
      } else {        
        outByteBuf = parser.parseRandom((ByteBuf)msg);
      }

      out.add(outByteBuf);
      return;
    }

    out.add(ReferenceCountUtil.retain(msg));
  }

  protected class Parser {    
    private int fieldDelim = ',';
    private int recordDelim = '\n';
    private int quoteDelim = '\"';
    private int columns;
    private int [] project;    
    private boolean skipHeader;
    private boolean seekRow;
    private long blockSize;
    private long offset;

    private boolean [] projectMask;
    public boolean isSorted;
    private int lastColumn;
    private byte[] leftOver = null;
    private long totalBytes;
    private long carryoverBytes;
    private int currentColumn = 0;
    private boolean currentUnderQuote;
    private byte[] outBytes = null;
    private byte[] inBytes = null;    
    
    private final int MAX_RECORDS = 1024;
    private int[] beginIndex = null;
    private int[] endIndex = null;

    // Strict implementation on https://tools.ietf.org/html/rfc4180 (RFC 4180)
    // Common usage of CSV is US-ASCII
    protected void init() {
      projectMask = new boolean[columns];
      for (int i = 0; i < columns; i++){
          projectMask[i] = false;
      }

      isSorted = true;
      int col = -1;
      for (int p : project){
        if (p > columns){
            throw new IllegalArgumentException(String.format("Invalid column [%d] ", p));
        }
        if(projectMask[p]) { // Duplicate columns
            throw new IllegalArgumentException(String.format("Duplicate column [%d] ", p));
        } else {
            projectMask[p] = true;
        }
        if (p <= col) {
          isSorted = false;
        }
        col = p;
      }

      lastColumn = col;
      totalBytes = 0;
      carryoverBytes = 0;
      currentColumn = 0;
      currentUnderQuote = false;
      
      if (offset == 0) {
        seekRow = skipHeader;
      } else {
        seekRow = true;
      }
      
      beginIndex = new int[columns * MAX_RECORDS];
      endIndex = new int[columns * MAX_RECORDS];
    }

    public Parser(String config, Long offset) {
        this.offset = offset;
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            InputSource is = new InputSource();
            is.setCharacterStream(new StringReader(config));
            Document dom = db.parse(is);
            dom.getDocumentElement().normalize();        
            Element configurationElement = (Element)dom.getElementsByTagName("Configuration").item(0);
            
            Element columnsElement = (Element)configurationElement.getElementsByTagName("Columns").item(0);
            this.columns = Integer.parseInt(columnsElement.getTextContent());

            Element projectElement = (Element)configurationElement.getElementsByTagName("Project").item(0);            
            String[] cols = projectElement.getTextContent().split(",");
            this.project = new int[cols.length];
            for (int i = 0; i < cols.length; i++) {
                this.project[i] = Integer.parseInt(cols[i].trim());
            }

            Element skipHeaderElement = (Element)configurationElement.getElementsByTagName("SkipHeader").item(0);
            this.skipHeader = Boolean.parseBoolean(skipHeaderElement.getTextContent()); 

            Element blockSizeElement = (Element)configurationElement.getElementsByTagName("BlockSize").item(0);
            this.blockSize = Long.parseLong(blockSizeElement.getTextContent());            
        } catch ( Exception e ) {
            throw new IllegalArgumentException(String.format("Invalid XML format "));
        }
        init();
    }

    /* Return last out_index */
    private int writeRecords(byte[] inBytes, int index, int bufferBytes, byte[] outBytes) {      
      boolean underQuote = false;
      int col = 0;
      int lastRow = index;
      int out_index = 0;      

      beginIndex[col] = index;
      while (index < bufferBytes) {        
        byte b = inBytes[index++];
        if (b == quoteDelim){
            underQuote = !underQuote;
        }
        if ((b == recordDelim || b == fieldDelim) && !underQuote ) {            
            endIndex[col] = index - 1;                
            if(++col == columns){ // End of record
              out_index = writeColumns(1, inBytes, outBytes, out_index);
              col = 0;                  
              totalBytes += index - lastRow;
              lastRow = index;
              if (blockSize > 0 && totalBytes > blockSize){ // is_done()
                return out_index;
              }
            }
            beginIndex[col] = index;            
        }
      }

      if (!is_done()) { // This check may be redundant
        // Preserve remaining data
        leftOver = Arrays.copyOfRange(inBytes, lastRow, bufferBytes);
      }      
      
      return out_index;
    }

    private int writeColumns(int recordCount, byte[] inBytes, byte[] outBytes, int out_index) {
      for(int fieldIndex = 0; fieldIndex < recordCount * columns; fieldIndex += columns) {
        for (int c : project ) {
          int col = c + fieldIndex;
          int length = endIndex[col] - beginIndex[col];
          System.arraycopy(inBytes, beginIndex[col], outBytes, out_index, length);
          out_index += length;
          outBytes[out_index++] = (byte) fieldDelim;
        }
        outBytes[out_index - 1] = (byte)recordDelim;
      }
      return out_index;
    }

    public boolean is_done() {
      if (blockSize > 0 && totalBytes > blockSize){
        return true;
      }
      return false;
    }

    /* Return number of of fully processed records */
    private int parseRecords(byte[] inBytes, int index, int bufferBytes) {
      boolean underQuote = false;
      int recordCount = 0;
      int fieldCount = 0;
      int col = 0;
      int lastRow = index;

      if (blockSize > 0 && totalBytes > blockSize){
        return 0;
      }

      beginIndex[fieldCount] = index;
      for (int i = index; i < bufferBytes; i ++) {        
        byte b = inBytes[i];
        if (b == quoteDelim){
            underQuote = !underQuote;
        }
        if (!underQuote) {
            if (b == recordDelim || b == fieldDelim) {
                endIndex[fieldCount] = i;
                fieldCount++;
                if(++col == columns){ // End of record
                  col = 0;
                  totalBytes += i+1 - lastRow;
                  lastRow = i+1;
                  if (++recordCount >= MAX_RECORDS){
                    break;
                  }
                  if (blockSize > 0 && totalBytes > blockSize){
                    break;
                  }
                }
                beginIndex[fieldCount] = i + 1;
            }
        }
      }      
      return recordCount;
    }

    public ByteBuf parseRandom(ByteBuf inByteBuf) {      
      int readableBytes = inByteBuf.readableBytes();
      int leftOverBytes = 0;      
      
      if (leftOver != null){
          leftOverBytes = leftOver.length;
      }
      int bufferBytes = readableBytes + leftOverBytes;
      
      if (outBytes == null || inBytes == null){
        outBytes = new byte[bufferBytes + 128];
        inBytes = new byte[bufferBytes + 128];
      }
      if (outBytes.length < bufferBytes || inBytes.length < bufferBytes) {
        outBytes = new byte[bufferBytes + 128];
        inBytes = new byte[bufferBytes + 128];
      }

      if (leftOverBytes > 0) {    
        System.arraycopy(leftOver, 0, inBytes, 0, leftOverBytes);        
      }      
      
      inByteBuf.getBytes(inByteBuf.readerIndex(), inBytes, leftOverBytes, readableBytes);      

      leftOver = null;

      int index, nextIndex, col, lastRow, outIndex;
      index = nextIndex = col = lastRow = outIndex = 0;

      if (seekRow) {
        seekRow = false;                     
        while(index < bufferBytes){
          byte b = inBytes[index++];
          if(b == (byte)recordDelim) {
            break;
          }
        }

        totalBytes += index - lastRow;
        lastRow = index;
      }
      
      // Main parsing loop     
      while (index < bufferBytes){
        int recordCount = parseRecords(inBytes, index, bufferBytes);
        if (recordCount <= 0){
          break;
        }
        outIndex = writeColumns(recordCount, inBytes, outBytes, outIndex);        
        index = endIndex[recordCount * columns - 1] + 1;
        lastRow = index;
        if (recordCount < MAX_RECORDS) { // End of data
          break;
        }          
      }
      
      if (!is_done()){
        // Preserve remaining data
        leftOver = Arrays.copyOfRange(inBytes, lastRow, bufferBytes);
      }

      ByteBuf outByteBuf = Unpooled.wrappedBuffer(outBytes, 0, outIndex);
      outByteBuf.retain();
      return outByteBuf;
    }

  public ByteBuf parseSorted(ByteBuf inByteBuf) {
      int outIndex = 0;
      int readableBytes = inByteBuf.readableBytes();                  
      int index = 0;
      int lastRow = index;

      if (outBytes == null || outBytes.length < readableBytes) {
        outBytes = new byte[readableBytes];
        inBytes = new byte[readableBytes];
      }

      inByteBuf.getBytes(inByteBuf.readerIndex(), inBytes, 0, readableBytes);

      if (seekRow) {
        seekRow = false;
        while(index < readableBytes){
          byte b = inBytes[index++];
          if(b == (byte)recordDelim) {
            break;
          }
        }

        totalBytes += index - lastRow;
        lastRow = index;
      }            
      
      if (carryoverBytes > 0){
        totalBytes += carryoverBytes;
        carryoverBytes = 0;
      }

      int col = currentColumn;
      boolean underQuote = currentUnderQuote;
      boolean do_project = projectMask[col];      
       while (index < readableBytes) {
        byte b = inBytes[index++];
        if (b == quoteDelim){
          underQuote = !underQuote;
        }
        if ((b == recordDelim || b == fieldDelim) && !underQuote ) {
          if (do_project) {
            if (col != lastColumn){
              outBytes[outIndex++] = (byte)fieldDelim;
            } else {
              outBytes[outIndex++] = (byte)recordDelim;
            }
          }
          if(++col == columns){ // End of record            
            //outBytes[outIndex - 1] = (byte)recordDelim;                          
            totalBytes += index - lastRow;
            lastRow = index;
            col = 0;
            if (is_done()){
              break;
            }
          }
          do_project = projectMask[col];
        } else if (do_project) {
          outBytes[outIndex++] = b;          
        }        
      }

      currentColumn = col;
      currentUnderQuote = underQuote;

      if(!is_done()){                
        carryoverBytes = index - lastRow;        
      }
      ByteBuf outByteBuf = Unpooled.wrappedBuffer(outBytes, 0, outIndex);
      outByteBuf.retain();
      return outByteBuf;
    }
  } // class parser  
}
