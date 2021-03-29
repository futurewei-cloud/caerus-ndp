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

  private Configuration conf;
  ProjectionPlugin(Configuration conf) {    
    this.conf = conf;    
  }

  @Override
  public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
    //System.out.println("ProjectionPlugin::handlerAdded");
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
    //System.out.println("ProjectionPlugin::handlerRemoved");    
  }

  @Override
  public boolean acceptOutboundMessage(Object msg) throws Exception {   
    if (!(msg instanceof ByteBuf)) {
      //System.out.println("ProjectionPlugin::acceptOutboundMessage " + msg.getClass().getName());
    } else {
      // io.netty.buffer.PooledUnsafeDirectByteBuf
      // System.out.println("ProjectionPlugin::acceptOutboundMessage " + msg.getClass().getName());
    }
    return super.acceptOutboundMessage(msg);
  }

  @Override
  public boolean acceptInboundMessage(Object msg) throws Exception {
    //System.out.println("ProjectionPlugin::acceptInboundMessage " + msg.getClass().getName());
    return super.acceptInboundMessage(msg);
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, Object msg, List<Object> out) throws Exception {
    //System.out.println("ProjectionPlugin::decode " + msg.getClass().getName());
    if (msg instanceof HttpRequest) {
      HttpRequest req = (HttpRequest)msg;      
      String ndpConfig = req.headers().get("NdpConfig");      
      if (ndpConfig == null){
        out.add(ReferenceCountUtil.retain(msg));
        return;
      }

      //System.out.println("ProjectionPlugin::decode ctx: " + ctx + " channel: " + ctx.channel());
      
      System.out.println(req.uri());

      long offset = 0L;
      System.out.println("ProjectionPlugin::decode ndpConfig " + ndpConfig);
      List<NameValuePair> params = URLEncodedUtils.parse(new URI(req.uri()), "UTF-8");
      for (NameValuePair param : params) {
        if (param.getName().equals("offset")) {
          //System.out.println(param.getName() + " : " + param.getValue());
          offset = Long.parseLong(param.getValue());
        }
      }
      try {
        Parser parser = new Parser(ndpConfig, offset);
        ctx.channel().attr(AttributeKey.valueOf("NdpParser")).set(parser);
        ctx.channel().attr(AttributeKey.valueOf("FlushInProgress")).set(false);
      } catch ( Exception e ) {
        System.out.println("Invalid parser NdpConfig");
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
        //ChannelFuture future = ctx.channel().close();        
        Boolean flushInProgress = (Boolean) ctx.channel().attr(AttributeKey.valueOf("FlushInProgress")).get();
        if(!flushInProgress){
          System.out.println("ProjectionPlugin::encode flushInProgress");
          ctx.channel().attr(AttributeKey.valueOf("FlushInProgress")).set(true);
          ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);

/*
          ChannelFuture future = ctx.writeAndFlush(Unpooled.EMPTY_BUFFER);
          future.addListener(new ChannelFutureListener() {
            public void operationComplete(ChannelFuture future) {
              ctx.channel().close();
            }
          });
*/          
        }        
 
        //ctx.flush();
        //ctx.close();

        //ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        //ReferenceCountUtil.release(msg);
        out.add(Unpooled.EMPTY_BUFFER);
        return;
      }

      ByteBuf inByteBuf = (ByteBuf)msg;
      //ByteBuf outByteBuf = inByteBuf.copy();
      ByteBuf outByteBuf = parser.parse(inByteBuf);
      //System.out.println(inByteBuf.toString(CharsetUtil.UTF_8));
      out.add(outByteBuf);

      return;
    }

    //System.out.println("ProjectionPlugin::encode " + msg.getClass().getName());
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
    private int byteCount = 0;
    private byte[] leftOver = null;
    private long totalBytes;
    private byte[] outBytes = null;
    private byte[] inBytes = null;
    public int rowCount = 0;

    // Strict implementation on https://tools.ietf.org/html/rfc4180 (RFC 4180)
    // Common usage of CSV is US-ASCII
    protected void init() {
      projectMask = new boolean[columns];
      for (int i = 0; i < columns; i++){
          projectMask[i] = false;
      }

      for (int i = 0; i < project.length; i++){
        if (project[i] > columns){
            throw new IllegalArgumentException(String.format("Invalid column [%d] ", project[i]));
        }
        if(projectMask[project[i]]) { // Duplicate columns
            throw new IllegalArgumentException(String.format("Duplicate column [%d] ", project[i]));
        } else {
            projectMask[project[i]] = true;
        }
      }
      totalBytes = 0;
      if (blockSize > 0 && offset > 0) {
        seekRow = true;
        skipHeader = false;
      } else {
        seekRow = false;
      }
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
            //System.out.println(columnsElement.getTextContent());
            this.columns = Integer.parseInt(columnsElement.getTextContent());

            Element projectElement = (Element)configurationElement.getElementsByTagName("Project").item(0);
            //System.out.println(projectElement.getTextContent());
            String[] cols = projectElement.getTextContent().split(",");
            this.project = new int[cols.length];
            for (int i = 0; i < cols.length; i++) {
                this.project[i] = Integer.parseInt(cols[i].trim());
            }

            Element skipHeaderElement = (Element)configurationElement.getElementsByTagName("SkipHeader").item(0);
            //System.out.println(skipHeaderElement.getTextContent());
            this.skipHeader = Boolean.parseBoolean(skipHeaderElement.getTextContent()); 

            Element blockSizeElement = (Element)configurationElement.getElementsByTagName("BlockSize").item(0);
            //System.out.println(blockSizeElement.getTextContent());
            this.blockSize = Long.parseLong(blockSizeElement.getTextContent());
        } catch ( Exception e ) {
            throw new IllegalArgumentException(String.format("Invalid XML format "));
        }
        init();
    }

    private class DataField {
      public int from;
      public int to;
      public DataField(int from, int to){
        this.from = from;
        this.to = to; // Includive
      }
    }

    /* Return index of next record or -1 on failure */
    private int next(byte[] inBytes, int index, int bufferBytes) {
      boolean underQuote = false;
      for (int i = index; i < bufferBytes; i ++) {
        byte b = inBytes[i];
        if (b == quoteDelim){
            underQuote = !underQuote;
        }
        if (!underQuote) {
            if (b == recordDelim || b == fieldDelim) {
                return i;
            }
        }
      }
      return -1;      
    }

    private int writeColumns(DataField[] fields, byte[] inBytes, byte[] outBytes, int outIndex) {
      int index = outIndex;
      int col;
      int length;
      int c;
      for (c = 0; c < project.length - 1; c++) {
        col = project[c];
        length = fields[col].to - fields[col].from;        
        System.arraycopy(inBytes, fields[col].from, outBytes, index, length);
        index += length;
        outBytes[index++] = (byte) fieldDelim;
      }

      col = project[c];
      length = fields[col].to - fields[col].from;
      System.arraycopy(inBytes, fields[col].from, outBytes, index, length);
      index += length;
      outBytes[index++] = (byte)recordDelim;

      return index;
    }

    public boolean is_done() {
      if (blockSize > 0 && totalBytes > blockSize){
        return true;
      }
      return false;
    }

    public ByteBuf parse(ByteBuf inByteBuf) {
      int readableBytes = inByteBuf.readableBytes();
      int leftOverBytes = 0;
      DataField[] fields = new DataField[columns];

      if (leftOver != null){
          leftOverBytes = leftOver.length;
      }
      int bufferBytes = readableBytes + leftOverBytes;
      
      if (outBytes == null || inBytes == null){
        outBytes = new byte[bufferBytes];
        inBytes = new byte[bufferBytes];
      }
      if (outBytes.length < bufferBytes || inBytes.length < bufferBytes) {
        outBytes = new byte[bufferBytes];
        inBytes = new byte[bufferBytes];
      }

      if (leftOverBytes > 0) {
          System.arraycopy(leftOver, 0, inBytes, 0, leftOverBytes);
      }
      // DirectByteBuffer.get(byte[] dest, int off, int len)
      inByteBuf.getBytes(inByteBuf.readerIndex(), inBytes, leftOverBytes, readableBytes);
      
      leftOver = null;

      int index, nextIndex, col, row_index, outIndex;
      index = nextIndex = col = row_index = outIndex = 0;
      if (seekRow) {
        seekRow = false;
        for (int i = index; i < bufferBytes; i ++) {
          if (inBytes[i] == recordDelim){
            index = i+1;
            totalBytes += (index - row_index );
            row_index = index;            
            break;
          }
        }
      }

      while (nextIndex < bufferBytes){
        nextIndex = next(inBytes, index, bufferBytes);
        if (nextIndex < 0) { // End of data
            break;
        }
        fields[col] = new DataField(index, nextIndex);
        index = nextIndex + 1;

        if (++col == columns) {
            if (inBytes[nextIndex] == fieldDelim) { // TBL use case
                if (index < bufferBytes){
                    index++; // Skip recordDelim
                } else {
                    break; // End of data
                }
            }
            if (skipHeader){
                skipHeader = false;
            } else {
                rowCount++;
                outIndex = writeColumns(fields, inBytes, outBytes, outIndex);
            }
            col = 0;
            // This row was fully processed
            totalBytes += (index - row_index);
            if (blockSize > 0 && totalBytes > blockSize){
/*              
              int tmpLength = fields[columns - 1].to -  fields[0].from;
              byte tmpBytes[] = new byte[tmpLength];
              System.arraycopy(inBytes, fields[0].from, tmpBytes, 0, tmpLength);
              System.out.println(new String(tmpBytes, StandardCharsets.UTF_8));
*/              
              ByteBuf outByteBuf = Unpooled.wrappedBuffer(outBytes, 0, outIndex);
              outByteBuf.retain();
              return outByteBuf;
            }
            row_index = index;
        }
      }

      // Preserve remaining data
      leftOver = Arrays.copyOfRange(inBytes, row_index, bufferBytes);
      ByteBuf outByteBuf = Unpooled.wrappedBuffer(outBytes, 0, outIndex);
      outByteBuf.retain();
      return outByteBuf;
    }
  }

  public static Configuration initializeState(Configuration conf) {
    System.out.println("ProjectionPlugin::initializeState");
    return(new Configuration(conf));
  }  
}
