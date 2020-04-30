package com.transport.lib.http.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.RequestContext;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.zookeeper.Utils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.transport.lib.TransportService.*;

public class HttpAsyncAndSyncRequestReceiver implements Runnable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(HttpAsyncAndSyncRequestReceiver.class);

    // HTTP async requests are processed by 3 receiver threads
    private static final ExecutorService service = Executors.newFixedThreadPool(3);

    private HttpServer server;

    @Override
    public void run() {

        try {
            server = HttpServer.create(Utils.getHttpBindAddress(), 0);
            server.createContext("/request", new HttpRequestHandler());
            server.setExecutor(Executors.newFixedThreadPool(3));
            server.start();
        } catch (IOException httpServerStartupException) {
            logger.error("Error during HTTP request receiver startup:", httpServerStartupException);
            throw new TransportSystemException(httpServerStartupException);
        }
        logger.info("{} terminated", this.getClass().getSimpleName());
    }

    @Override
    public void close() {
        server.stop(2);
        logger.info("HTTP request receiver stopped");
    }

    private class HttpRequestHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange request) throws IOException {
            // New Kryo instance per thread
            Kryo kryo = new Kryo();
            // Unmarshal message to Command object
            Input input = new Input(request.getRequestBody());
            final Command command = kryo.readObject(input, Command.class);
            // If it is async request - answer with "OK" message before target method invocation
            if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                String response = "OK";
                request.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = request.getResponseBody();
                os.write(response.getBytes());
                os.close();
                request.close();
            }
            // If it is async request - start target method invocation in separate thread
            if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                Runnable runnable = () -> {
                    try {
                        // Target method will be executed in current Thread, so set service metadata
                        // like client's module.id and SecurityTicket token in ThreadLocal variables
                        RequestContext.setSourceModuleId(command.getSourceModuleId());
                        RequestContext.setSecurityTicket(command.getTicket());
                        // Invoke target method and receive result
                        Object result = invoke(command);
                        // Marshall result as CallbackContainer
                        ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                        Output output = new Output(bOutput);
                        // Construct CallbackContainer and marshall it to output stream
                        kryo.writeObject(output, constructCallbackContainer(command, result));
                        output.close();

                        CloseableHttpClient client = HttpClientBuilder.create().build();
                        HttpPost httpPost = new HttpPost(command.getCallBackZMQ() + "/response");
                        HttpEntity postParams = new ByteArrayEntity(bOutput.toByteArray());
                        httpPost.setEntity(postParams);
                        CloseableHttpResponse httpResponse = client.execute(httpPost);
                        int response = httpResponse.getStatusLine().getStatusCode();
                        client.close();
                        if (response != 200) {
                            throw new TransportExecutionException("Response for RPC request " + command.getRqUid() + " returned status " + response);
                        }
                    } catch (ClassNotFoundException | NoSuchMethodException | IOException e) {
                        logger.error("Error while receiving async request");
                        throw new TransportExecutionException(e);
                    }
                };
                service.execute(runnable);
            } else {
                // Target method will be executed in current Thread, so set service metadata
                // like client's module.id and SecurityTicket token in ThreadLocal variables
                RequestContext.setSourceModuleId(command.getSourceModuleId());
                RequestContext.setSecurityTicket(command.getTicket());
                // Invoke target method and receive result
                Object result = invoke(command);
                ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                Output output = new Output(bOutput);
                // Marshall result
                kryo.writeClassAndObject(output, getResult(result));
                output.close();
                byte[] response = bOutput.toByteArray();
                request.sendResponseHeaders(200, response.length);
                OutputStream os = request.getResponseBody();
                os.write(response);
                os.close();
                request.close();
            }
        }
    }

}
