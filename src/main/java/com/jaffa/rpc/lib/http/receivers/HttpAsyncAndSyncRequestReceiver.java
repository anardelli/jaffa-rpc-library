package com.jaffa.rpc.lib.http.receivers;

import com.google.common.io.ByteStreams;
import com.jaffa.rpc.lib.JaffaService;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.entities.RequestContext;
import com.jaffa.rpc.lib.exception.TransportExecutionException;
import com.jaffa.rpc.lib.exception.TransportSystemException;
import com.jaffa.rpc.lib.serialization.Serializer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.jaffa.rpc.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class HttpAsyncAndSyncRequestReceiver implements Runnable, Closeable {

    public static final CloseableHttpClient client;
    private static final ExecutorService service = Executors.newFixedThreadPool(3);

    static {
        PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(200);
        client = HttpClients.custom().setConnectionManager(connManager).build();
    }

    private HttpServer server;

    @Override
    public void run() {

        try {
            server = HttpServer.create(Utils.getHttpBindAddress(), 0);
            server.createContext("/request", new HttpRequestHandler());
            server.setExecutor(Executors.newFixedThreadPool(9));
            server.start();
        } catch (IOException httpServerStartupException) {
            log.error("Error during HTTP request receiver startup:", httpServerStartupException);
            throw new TransportSystemException(httpServerStartupException);
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
                            throw new TransportExecutionException("Response for RPC request " + command.getRqUid() + " returned status " + response);
                        }
                    } catch (ClassNotFoundException | NoSuchMethodException | IOException e) {
                        log.error("Error while receiving async request");
                        throw new TransportExecutionException(e);
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
