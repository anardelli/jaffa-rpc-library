package com.jaffa.rpc.lib.http.receivers;

import com.google.common.io.ByteStreams;
import com.jaffa.rpc.lib.JaffaService;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.entities.RequestContext;
import com.jaffa.rpc.lib.exception.JaffaRpcExecutionException;
import com.jaffa.rpc.lib.exception.JaffaRpcSystemException;
import com.jaffa.rpc.lib.serialization.Serializer;
import com.jaffa.rpc.lib.zookeeper.Utils;
import com.sun.net.httpserver.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.*;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class HttpAsyncAndSyncRequestReceiver implements Runnable, Closeable {

    private static final ExecutorService service = Executors.newFixedThreadPool(3);
    public static CloseableHttpClient client;
    private HttpServer server;

    public static void initClient() {
        if (Boolean.parseBoolean(System.getProperty("jaffa.rpc.protocol.use.https", "false"))) {
            TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
            SSLContext sslContext;
            try {
                sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
            } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
                log.error("Error occurred while creating HttpClient", e);
                throw new JaffaRpcSystemException(e);
            }
            SSLConnectionSocketFactory sslConnectionSocketFactory = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
            Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create().register("https", sslConnectionSocketFactory).build();
            PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
            connectionManager.setMaxTotal(200);
            client = HttpClients.custom().setSSLSocketFactory(sslConnectionSocketFactory).setConnectionManager(connectionManager).build();
        } else {
            PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
            connManager.setMaxTotal(200);
            client = HttpClients.custom().setConnectionManager(connManager).build();
        }
    }

    @Override
    public void run() {
        try {
            if (Boolean.parseBoolean(System.getProperty("jaffa.rpc.protocol.use.https", "false"))) {
                HttpsServer httpsServer = HttpsServer.create(Utils.getHttpBindAddress(), 0);
                SSLContext sslContext = SSLContext.getInstance("TLS");
                KeyStore ks = KeyStore.getInstance("PKCS12");
                char[] storepass = System.getProperty("jaffa.rpc.protocol.https.storepass").toCharArray();
                FileInputStream fis = new FileInputStream(System.getProperty("jaffa.rpc.protocol.https.keystore"));
                ks.load(fis, storepass);
                KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
                kmf.init(ks, storepass);
                TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
                tmf.init(ks);
                sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
                httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
                    @Override
                    public void configure(HttpsParameters params) {
                        try {
                            SSLContext c = SSLContext.getDefault();
                            SSLEngine engine = c.createSSLEngine();
                            params.setNeedClientAuth(true);
                            params.setCipherSuites(engine.getEnabledCipherSuites());
                            params.setProtocols(engine.getEnabledProtocols());
                            SSLParameters defaultSSLParameters = c.getDefaultSSLParameters();
                            params.setSSLParameters(defaultSSLParameters);
                        } catch (Exception ex) {
                            log.error("Failed to create Jaffa HTTPS server", ex);
                            throw new JaffaRpcSystemException(ex);
                        }
                    }
                });
                server = httpsServer;
            } else {
                server = HttpServer.create(Utils.getHttpBindAddress(), 0);
            }
            server.createContext("/request", new HttpRequestHandler());
            server.setExecutor(Executors.newFixedThreadPool(9));
            server.start();
        } catch (Exception httpServerStartupException) {
            log.error("Error during HTTP request receiver startup:", httpServerStartupException);
            throw new JaffaRpcSystemException(httpServerStartupException);
        }
        log.info("{} started", this.getClass().getSimpleName());
    }

    @Override
    public void close() {
        server.stop(2);
        service.shutdown();
        try {
            client.close();
        } catch (IOException e) {
            log.error("Error while closing HTTP client", e);
        }
        log.info("HTTP request receiver stopped");
    }

    private class HttpRequestHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange request) throws IOException {
            final Command command = Serializer.getCtx().deserialize(ByteStreams.toByteArray(request.getRequestBody()), Command.class);
            if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                String response = "OK";
                request.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = request.getResponseBody();
                os.write(response.getBytes());
                os.close();
                request.close();
            }
            if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                Runnable runnable = () -> {
                    try {
                        RequestContext.setSourceModuleId(command.getSourceModuleId());
                        RequestContext.setSecurityTicket(command.getTicket());
                        Object result = JaffaService.invoke(command);
                        byte[] serializedResponse = Serializer.getCtx().serialize(JaffaService.constructCallbackContainer(command, result));
                        HttpPost httpPost = new HttpPost(command.getCallBackZMQ() + "/response");
                        HttpEntity postParams = new ByteArrayEntity(serializedResponse);
                        httpPost.setEntity(postParams);
                        CloseableHttpResponse httpResponse = client.execute(httpPost);
                        int response = httpResponse.getStatusLine().getStatusCode();
                        httpResponse.close();
                        if (response != 200) {
                            throw new JaffaRpcExecutionException("Response for RPC request " + command.getRqUid() + " returned status " + response);
                        }
                    } catch (ClassNotFoundException | NoSuchMethodException | IOException e) {
                        log.error("Error while receiving async request");
                        throw new JaffaRpcExecutionException(e);
                    }
                };
                service.execute(runnable);
            } else {
                RequestContext.setSourceModuleId(command.getSourceModuleId());
                RequestContext.setSecurityTicket(command.getTicket());
                Object result = JaffaService.invoke(command);
                byte[] response = Serializer.getCtx().serializeWithClass(JaffaService.getResult(result));
                request.sendResponseHeaders(200, response.length);
                OutputStream os = request.getResponseBody();
                os.write(response);
                os.close();
                request.close();
            }
        }
    }
}
