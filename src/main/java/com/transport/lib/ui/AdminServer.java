package com.transport.lib.ui;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.transport.lib.entities.Command;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.commons.collections4.QueueUtils;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Queue;
import java.util.concurrent.Executors;

@Component
public class AdminServer {

    @Getter
    @AllArgsConstructor
    public static class ResponseMetric{
        private final long time;
        private final long duration;
    }

    private static final Logger logger = LoggerFactory.getLogger(AdminServer.class);

    private HttpServer server;

    // Keep last 1000 responses
    public static final Queue<ResponseMetric> responses = QueueUtils.synchronizedQueue(new CircularFifoQueue<>(1000));

    public static void addMetric(Command command){
        long executionDuration = System.currentTimeMillis() - command.getRequestTime();
        logger.info(">>>>>> Executed request {} in {} ms", command.getRqUid(), executionDuration);
        responses.add(new ResponseMetric(command.getRequestTime(), executionDuration));
    }

    private void respondWithFile(HttpExchange exchange, String fileName) throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(fileName);
        if(is == null) throw new IOException("No such file in resources: " + fileName);
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[16384];
        while ((nRead = is.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }
        byte[] page = buffer.toByteArray();
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
            server = HttpServer.create(new InetSocketAddress(InetAddress.getLocalHost(), getFreePort()), 0);
            server.createContext("/", (HttpExchange exchange) -> {
                String path = exchange.getRequestURI().getPath();
                if ("/admin".equals(path)) {
                    respondWithFile(exchange, "admin.html");
                } else if ("/vis.min.css".equals(path)) {
                    respondWithFile(exchange, "vis.min.css");
                } else if ("/vis.min.js".equals(path)) {
                    respondWithFile(exchange, "vis.min.js");
                }else if ("/response".equals(path)) {
                    int count = 0;
                    StringBuilder builder = new StringBuilder();
                    ResponseMetric metric;
                    do{
                        metric = responses.poll();
                        if(metric != null){
                            count++;
                            builder.append(metric.getTime()).append(':').append(metric.getDuration()).append(';');
                        }
                    } while(metric != null && count < 30);
                    respondWithString(exchange, builder.toString());
                } else {
                    respondWithString(exchange, "OK");
                }
            });
            server.setExecutor(Executors.newFixedThreadPool(3));
            server.start();
            logger.info("Transport console started at {}", "http://" + server.getAddress().getHostName() + ":" + server.getAddress().getPort());
        } catch (IOException httpServerStartupException) {
            logger.error("Exception during admin HTTP server startup", httpServerStartupException);
        }
    }

    @PreDestroy
    public void destroy() {
        if (server != null) {
            server.stop(2);
        }
    }
}
