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

//package org.apache.hadoop.hdfs.web;
package org.dike.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;
import java.net.URL;
import java.net.URI;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.lang.reflect.InvocationTargetException;
import java.security.PrivilegedExceptionAction;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryUtils;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics;
import org.apache.hadoop.hdfs.DFSOpsCountStatistics.OpType;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.resources.*;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam.Op;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.util.StringUtils;

//import static org.apache.hadoop.fs.impl.PathCapabilitiesSupport.validatePathCapabilityArgs;

public class NdpHdfsFileSystem extends WebHdfsFileSystem {
    public static final String NdpHDFS_SCHEME = "ndphdfs";
    private UserGroupInformation ugi;
    private URI uri = null;
    private boolean isInsecureCluster;

    @Override
    public synchronized void initialize(URI uri, Configuration conf
    ) throws IOException {
        this.ugi = UserGroupInformation.getCurrentUser();
        this.uri = uri;
        this.isInsecureCluster = !UserGroupInformation.isSecurityEnabled();

        super.initialize(uri, conf);
    }

    public FSDataInputStream open(final Path fspath, final int bufferSize,
                                  final String ndpConfig) throws IOException {
        if(ndpConfig == null) {
            return super.open(fspath, bufferSize);
        }

        statistics.incrementReadOps(1);
        NdpHdfsInputStream NdpHdfsfsInputStream =
                new NdpHdfsInputStream(fspath, bufferSize, ndpConfig);

        return new FSDataInputStream(NdpHdfsfsInputStream);
    }

    @Override
    public String getScheme() {
        return NdpHDFS_SCHEME;
    }

    private static Map<?, ?> validateResponse(final HttpOpParam.Op op,
                                              final HttpURLConnection conn, boolean unwrapException)
            throws IOException {
        final int code = conn.getResponseCode();

        return null;
    }

    public class NdpHdfsInputStream extends FSInputStream {
        private NdpReadRunner readRunner = null;
        NdpHdfsInputStream(Path path, int buffersize, String ndpConfig ) throws IOException {
            readRunner = new NdpReadRunner(path, buffersize, ndpConfig);
        }
        @Override
        public int read() throws IOException {
            final byte[] b = new byte[1];
            return (read(b, 0, 1) == -1) ? -1 : (b[0] & 0xff);
        }

        @Override
        public int read(byte b[], int off, int len) throws IOException {
            return readRunner.read(b, off, len);
        }

        @Override
        public void seek(long newPos) throws IOException {
            readRunner.seek(newPos);
        }

        @Override
        public long getPos() throws IOException {
            return readRunner.getPos();
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException {
            return false;
        }

        @Override
        public void close() throws IOException {
            readRunner.close();
        }

        public void setFileLength(long len) {
            readRunner.setFileLength(len);
        }
        public long getFileLength() {
            return readRunner.getFileLength();
        }
    }

    enum RunnerState {
        DISCONNECTED, // Connection is closed programmatically by ReadRunner
        OPEN,         // Connection has been established by ReadRunner
        SEEK,         // Calling code has explicitly called seek()
        CLOSED        // Calling code has explicitly called close()
    }

    private static final String OFFSET_PARAM_PREFIX = OffsetParam.NAME + "=";
    static URL removeOffsetParam(final URL url) throws MalformedURLException {
        String query = url.getQuery();
        if (query == null) {
            return url;
        }
        final String lower = StringUtils.toLowerCase(query);
        if (!lower.startsWith(OFFSET_PARAM_PREFIX)
                && !lower.contains("&" + OFFSET_PARAM_PREFIX)) {
            return url;
        }

        //rebuild query
        StringBuilder b = null;
        for(final StringTokenizer st = new StringTokenizer(query, "&");
            st.hasMoreTokens();) {
            final String token = st.nextToken();
            if (!StringUtils.toLowerCase(token).startsWith(OFFSET_PARAM_PREFIX)) {
                if (b == null) {
                    b = new StringBuilder("?").append(token);
                } else {
                    b.append('&').append(token);
                }
            }
        }
        query = b == null? "": b.toString();

        final String urlStr = url.toString();
        return new URL(urlStr.substring(0, urlStr.indexOf('?')) + query);
    }

    private synchronized Param<?, ?>[] getAuthParameters(final HttpOpParam.Op op)
            throws IOException {
        List<Param<?,?>> authParams = new ArrayList<Param<?,?>>(); //Lists.newArrayList();
        // Skip adding delegation token for token operations because these
        // operations require authentication.
        Token<?> token = null;
        if (!op.getRequireAuth()) {
            token = getDelegationToken();
        }
        if (token != null) {
            authParams.add(new DelegationParam(token.encodeToUrlString()));
        } else {
            UserGroupInformation userUgi = ugi;
            UserGroupInformation realUgi = userUgi.getRealUser();
            if (realUgi != null) { // proxy user
                authParams.add(new DoAsParam(userUgi.getShortUserName()));
                userUgi = realUgi;
            }
            UserParam userParam = new UserParam((userUgi.getShortUserName()));

            //in insecure, use user.name parameter, in secure, use spnego auth
            if(isInsecureCluster) {
                authParams.add(userParam);
            }
        }
        return authParams.toArray(new Param<?,?>[0]);
    }

    protected class NdpReadRunner extends NdpAbstractRunner<Integer> {
        private String ndpConfig = null;
        private RunnerState runnerState = RunnerState.SEEK;
        private HttpURLConnection cachedConnection = null;
        private long fileLength = 0;
        private long pos = 0;
        private URL originalUrl = null;
        private URL resolvedUrl = null;
        private final Path path;
        private final int bufferSize;
        private InputStream in = null;
        private byte[] readBuffer;
        private int readOffset;
        private int readLength;

        NdpReadRunner(Path fspath, int bs, String ndpConfig) throws IOException {
            super(GetOpParam.Op.OPEN, false);
            this.ndpConfig = ndpConfig;
            this.path = fspath;
            this.bufferSize = bs;
            super.UpdateParameters(fspath, new BufferSizeParam(bs));

            getRedirectedUrl();
        }

        private void getRedirectedUrl() throws IOException {
            NdpURLRunner urlRunner = new NdpURLRunner(GetOpParam.Op.OPEN, null, false,
                    false , ndpConfig ) {
                @Override
                protected URL getUrl() throws IOException {
                    return toUrl(op, path, new BufferSizeParam(bufferSize));
                }
            };

            HttpURLConnection conn = urlRunner.run();
            String location = conn.getHeaderField("Location");
            if (location != null) {
                resolvedUrl = removeOffsetParam(new URL(location));
            } else {
                cachedConnection = conn;
            }
            originalUrl = super.getUrl();
        }

        int read(byte[] b, int off, int len) throws IOException {
            if (runnerState == RunnerState.CLOSED) {
                throw new IOException("Stream closed");
            }
            if (len == 0) {
                return 0;
            }

            if (runnerState == RunnerState.SEEK) {
                try {
                    final URL rurl = new URL(resolvedUrl + "&" + new OffsetParam(pos));
                    cachedConnection = new NdpURLRunner(GetOpParam.Op.OPEN, rurl, true,
                            false , ndpConfig ).run();
                } catch (IOException ioe) {
                    closeInputStream(RunnerState.DISCONNECTED);
                }
            }

            readBuffer = b;
            readOffset = off;
            readLength = len;

            int count = -1;
            count = this.run();
            if (count >= 0) {
                statistics.incrementBytesRead(count);
                pos += count;
            }

            return count;
        }

        public void close() throws IOException {
            closeInputStream(RunnerState.CLOSED);
        }

        void seek(long newPos) throws IOException {
            if (pos != newPos) {
                pos = newPos;
                closeInputStream(RunnerState.SEEK);
            }
        }

        @Override
        protected URL getUrl() throws IOException {
            // This method is called every time either a read is executed.
            // The check for connection == null is to ensure that a new URL is only
            // created upon a new connection and not for every read.
            if (cachedConnection == null) {
                // Update URL with current offset. BufferSize doesn't change, but it
                // still must be included when creating the new URL.
                updateURLParameters(new BufferSizeParam(bufferSize),
                        new OffsetParam(pos));
                originalUrl = super.getUrl();
            }
            return originalUrl;
        }

        @Override
        protected HttpURLConnection connect(URL url) throws IOException {
            HttpURLConnection conn = cachedConnection;
            if (conn == null) {
                try {
                    conn = super.connect(url);
                } catch (IOException e) {
                    closeInputStream(RunnerState.DISCONNECTED);
                    throw e;
                }
            }
            return conn;
        }

        @Override
        Integer getResponse(final HttpURLConnection conn) throws IOException {
            try {
                cachedConnection = conn;
                if (in == null) {
                    in = initializeInputStream(conn);
                }

                int count = in.read(readBuffer, readOffset, readLength);
                return Integer.valueOf(count);
            } catch (IOException e) {
                String redirectHost = resolvedUrl.getAuthority();
                if (excludeDatanodes.getValue() != null) {
                    excludeDatanodes = new ExcludeDatanodesParam(redirectHost + ","
                            + excludeDatanodes.getValue());
                } else {
                    excludeDatanodes = new ExcludeDatanodesParam(redirectHost);
                }

                closeInputStream(RunnerState.DISCONNECTED);
                throw e;
            }
        }

        InputStream initializeInputStream(HttpURLConnection conn)
                throws IOException {
            resolvedUrl = removeOffsetParam(conn.getURL());
            final String cl = conn.getHeaderField(HttpHeaders.CONTENT_LENGTH);
            InputStream inStream = conn.getInputStream();
            if (cl != null) {
                long streamLength = Long.parseLong(cl);
                fileLength = pos + streamLength;
                // Java has a bug with >2GB request streams.  It won't bounds check
                // the reads so the transfer blocks until the server times out
                inStream = new BoundedInputStream(inStream, streamLength);
            } else {
                fileLength = -1; //getHdfsFileStatus(path).getLen();
            }
            // Wrapping in BufferedInputStream because it is more performant than
            // BoundedInputStream by itself.
            runnerState = RunnerState.OPEN;
            return new BufferedInputStream(inStream, bufferSize);
        }

        // Close both the InputStream and the connection.
        void closeInputStream(RunnerState rs) throws IOException {
            if (in != null) {
                IOUtils.close(cachedConnection);
                in = null;
            }
            cachedConnection = null;
            runnerState = rs;
        }

        long getFileLength() {
            return fileLength;
        }
        void setFileLength(long len) {
            fileLength = len;
        }
        long getPos() {
            return pos;
        }
    }

    class NdpURLRunner extends NdpAbstractRunner<HttpURLConnection> {
        private final URL url;
        @Override
        protected URL getUrl() throws IOException {
            return url;
        }

        protected NdpURLRunner(final HttpOpParam.Op op, final URL url,
                               boolean redirected, boolean followRedirect,
                               String ndpConfig) {
            super(op, redirected, followRedirect, ndpConfig);
            this.url = url;
        }

        @Override
        HttpURLConnection getResponse(HttpURLConnection conn) throws IOException {
            return conn;
        }
    }

    URL toUrl(final HttpOpParam.Op op, final Path fspath,
              final Param<?,?>... parameters) throws IOException {

        String urlString = "http://" + uri.getAuthority() + PATH_PREFIX
                + makeQualified(fspath).toUri().getRawPath();

        //HttpOpParam.Op op = GetOpParam.Op.OPEN;
        String query = "?" + op.toQueryString()
                + Param.toSortedString("&", getAuthParameters(op))
                + Param.toSortedString("&", parameters);

        return new URL(urlString + query);
    }

    abstract class NdpAbstractRunner<T> {
        protected final HttpOpParam.Op op;
        private final boolean redirected;
        protected ExcludeDatanodesParam excludeDatanodes =
                new ExcludeDatanodesParam("");

        private boolean checkRetry;
        private String redirectHost;
        private String ndpConfig;
        private boolean followRedirect = true;

        protected Path fspath;
        protected Param<?,?>[] parameters;

        protected void UpdateParameters(final Path fspath,
                                        Param<?,?>... parameters) {
            this.fspath = fspath;
            this.parameters = parameters;
        }

        protected void updateURLParameters(Param<?, ?>... p) {
            this.parameters = p;
        }

        protected URL getUrl() throws IOException {
            if (excludeDatanodes.getValue() != null) {
                Param<?, ?>[] tmpParam = new Param<?, ?>[parameters.length + 1];
                System.arraycopy(parameters, 0, tmpParam, 0, parameters.length);
                tmpParam[parameters.length] = excludeDatanodes;
                return toUrl(op, fspath, tmpParam);
            } else {
                return toUrl(op, fspath, parameters);
            }
        }

        protected NdpAbstractRunner(final HttpOpParam.Op op, boolean redirected) {
            this.op = op;
            this.redirected = redirected;
        }

        protected NdpAbstractRunner(final HttpOpParam.Op op, boolean redirected,
                                    boolean followRedirect, String ndpConfig) {
            this(op, redirected);
            this.followRedirect = followRedirect;
            this.ndpConfig = ndpConfig;
        }

        T run() throws IOException {
            UserGroupInformation connectUgi = ugi.getRealUser();
            if (connectUgi == null) {
                connectUgi = ugi;
            }
            if (op.getRequireAuth()) {
                connectUgi.checkTGTAndReloginFromKeytab();
            }
            try {
                return connectUgi.doAs(
                        new PrivilegedExceptionAction<T>() {
                            @Override
                            public T run() throws IOException {
                                return runWithRetry();
                            }
                        });
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        protected HttpURLConnection connect(URL url) throws IOException {
            redirectHost = null;

            if (op.getRedirect() && !redirected) {
                final HttpOpParam.Op redirectOp =
                        HttpOpParam.TemporaryRedirectOp.valueOf(op);
                final HttpURLConnection conn = connect(redirectOp, url);
                if (conn.getResponseCode() == op.getExpectedHttpResponseCode()) {
                    return conn;
                }
                try {
                    validateResponse(redirectOp, conn, false);
                    url = new URL(conn.getHeaderField("Location"));
                    redirectHost = url.getHost() + ":" + url.getPort();
                } finally {
                    conn.disconnect();
                }
                if (!followRedirect) {
                    return conn;
                }
            }
            try {
                final HttpURLConnection conn = connect(op, url);
                if (!op.getDoOutput()) {
                    validateResponse(op, conn, false);
                }
                return conn;
            } catch (IOException ioe) {
                if (redirectHost != null) {
                    if (excludeDatanodes.getValue() != null) {
                        excludeDatanodes = new ExcludeDatanodesParam(redirectHost + ","
                                + excludeDatanodes.getValue());
                    } else {
                        excludeDatanodes = new ExcludeDatanodesParam(redirectHost);
                    }
                }
                throw ioe;
            }
        }

        private HttpURLConnection connect(final HttpOpParam.Op op, final URL url)
                throws IOException {
            final HttpURLConnection conn =
                    (HttpURLConnection)connectionFactory.openConnection(url);
            final boolean doOutput = op.getDoOutput();
            conn.setRequestMethod(op.getType().toString());
            conn.setInstanceFollowRedirects(false);
            conn.setRequestProperty(EZ_HEADER, "true");
            conn.setRequestProperty("NdpConfig", ndpConfig);
            conn.setDoOutput(doOutput);
            conn.connect();
            return conn;
        }

        private T runWithRetry() throws IOException {
            for(int retry = 0; ; retry++) {
                checkRetry = !redirected;
                final URL url = getUrl();
                try {
                    final HttpURLConnection conn = connect(url);
                    return getResponse(conn);
                } catch (AccessControlException ace) {
                    throw ace;
                } catch (InvalidToken it) {
                    throw it;
                } catch (IOException ioe) {
                    throw ioe;
                }
            }
        }

        abstract T getResponse(HttpURLConnection conn) throws IOException;
    }
}
