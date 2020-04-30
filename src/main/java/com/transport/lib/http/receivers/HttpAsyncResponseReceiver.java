package com.transport.lib.http.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.transport.lib.common.FinalizationWorker;
import com.transport.lib.entities.CallbackContainer;
import com.transport.lib.entities.ExceptionHolder;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.zookeeper.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executors;

public class HttpAsyncResponseReceiver implements Runnable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(HttpAsyncResponseReceiver.class);

    private HttpServer server;

    @Override
    public void run() {
        try {
            server = HttpServer.create(Utils.getHttpCallbackBindAddress(),0);
            server.createContext("/response", new HttpRequestHandler());
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
        logger.info("HTTP async response receiver stopped");
    }

    private class HttpRequestHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange request) throws IOException {
            // New Kryo instance per thread
            Kryo kryo = new Kryo();
            try {
                // Receive raw bytes
                Input input = new Input(request.getRequestBody());
                // Unmarshal bytes to CallbackContainer
                CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                // Get target callback class
                Class<?> callbackClass = Class.forName(callbackContainer.getListener());
                // If request is still valid
                if (FinalizationWorker.eventsToConsume.remove(callbackContainer.getKey()) != null) {
                    // Send result to callback by invoking appropriate method
                    if (callbackContainer.getResult() instanceof ExceptionHolder) {
                        Method method = callbackClass.getMethod("onError", String.class, Throwable.class);
                        method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), new TransportExecutionException(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
                    } else {
                        Method method = callbackClass.getMethod("onSuccess", String.class, Class.forName(callbackContainer.getResultClass()));
                        if (Class.forName(callbackContainer.getResultClass()).equals(Void.class)) {
                            method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), null);
                        } else
                            method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                    }
                } else {
                    logger.warn("Response {} already expired", callbackContainer.getKey());
                }
                String response = "OK";
                request.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = request.getResponseBody();
                os.write(response.getBytes());
                os.close();
                request.close();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException callbackExecutionException) {
                logger.error("ZMQ callback execution exception", callbackExecutionException);
                throw new TransportExecutionException(callbackExecutionException);
            }

        }
    }
}
