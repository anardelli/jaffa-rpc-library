package com.jaffa.rpc.lib.ui;

import com.google.common.io.ByteStreams;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.zookeeper.Utils;
import com.sun.net.httpserver.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.QueueUtils;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.net.ssl.*;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Queue;
import java.util.concurrent.Executors;

@Slf4j
@Component
public class AdminServer {

    private static final Queue<ResponseMetric> responses = QueueUtils.synchronizedQueue(new CircularFifoQueue<>(1000));
    private HttpServer server;

    public static void addMetric(Command command) {
        double executionDuration = (System.nanoTime() - command.getLocalRequestTime()) / 1000000.0;
        log.info(">>>>>> Executed request {} in {} ms", command.getRqUid(), executionDuration);
        responses.add(new ResponseMetric(command.getRequestTime(), executionDuration));
    }

    private void respondWithFile(HttpExchange exchange, String fileName) throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(fileName);
        if (is == null) throw new IOException("No such file in resources: " + fileName);
        byte[] page = ByteStreams.toByteArray(is);
        exchange.sendResponseHeaders(200, page.length);
        OutputStream os = exchange.getResponseBody();
        os.write(page);
        os.close();
        exchange.close();
    }

    private void respondWithString(HttpExchange exchange, String response) throws IOException {
        exchange.sendResponseHeaders(200, response.getBytes().length);
        OutputStream os = exchange.getResponseBody();
        os.write(response.getBytes());
        os.close();
        exchange.close();
    }

    private Integer getFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @PostConstruct
    public void init() {
        try {
            boolean useHttps = Boolean.parseBoolean(System.getProperty("jaffa.admin.use.https", "false"));
            if (useHttps) {
                HttpsServer httpsServer = HttpsServer.create(new InetSocketAddress(Utils.getLocalHost(), getFreePort()), 0);
                SSLContext sslContext = SSLContext.getInstance("TLS");
                KeyStore ks = KeyStore.getInstance("PKCS12");
                char[] storepass = System.getProperty("jaffa.admin.storepass").toCharArray();
                FileInputStream fis = new FileInputStream(Utils.getRequiredOption("jaffa.admin.keystore"));
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
                        }
                    }
                });
                server = httpsServer;
            } else {
                server = HttpServer.create(new InetSocketAddress(Utils.getLocalHost(), getFreePort()), 0);
            }
            server.createContext("/", (HttpExchange exchange) -> {
                String path = exchange.getRequestURI().getPath();
                if ("/admin".equals(path)) {
                    respondWithFile(exchange, "admin.html");
                } else if ("/vis.min.css".equals(path)) {
                    respondWithFile(exchange, "vis.min.css");
                } else if ("/vis.min.js".equals(path)) {
                    respondWithFile(exchange, "vis.min.js");
                } else if ("/protocol".equals(path)) {
                    respondWithString(exchange, Utils.getRpcProtocol().getFullName());
                } else if ("/response".equals(path)) {
                    int count = 0;
                    StringBuilder builder = new StringBuilder();
                    ResponseMetric metric;
                    do {
                        metric = responses.poll();
                        if (metric != null) {
                            count++;
                            builder.append(metric.getTime()).append(':').append(metric.getDuration()).append(';');
                        }
                    } while (metric != null && count < 30);
                    respondWithString(exchange, builder.toString());
                } else {
                    respondWithString(exchange, "OK");
                }
            });
            server.setExecutor(Executors.newFixedThreadPool(3));
            server.start();
            log.info("Jaffa RPC console started at {}", (useHttps ? "https://" : "http://") + server.getAddress().getHostName() + ":" + server.getAddress().getPort() + "/admin");
        } catch (IOException | KeyStoreException | NoSuchAlgorithmException | CertificateException | KeyManagementException | UnrecoverableKeyException httpServerStartupException) {
            log.error("Exception during admin HTTP server startup", httpServerStartupException);
        }
    }

    @PreDestroy
    public void destroy() {
        if (server != null) {
            server.stop(2);
        }
    }

    @Getter
    @AllArgsConstructor
    public static class ResponseMetric {
        private final long time;
        private final double duration;
    }
}
