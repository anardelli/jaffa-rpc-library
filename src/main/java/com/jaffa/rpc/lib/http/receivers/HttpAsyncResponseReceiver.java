package com.jaffa.rpc.lib.http.receivers;

import com.google.common.io.ByteStreams;
import com.jaffa.rpc.lib.common.FinalizationWorker;
import com.jaffa.rpc.lib.entities.CallbackContainer;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.entities.ExceptionHolder;
import com.jaffa.rpc.lib.exception.JaffaRpcExecutionException;
import com.jaffa.rpc.lib.exception.JaffaRpcSystemException;
import com.jaffa.rpc.lib.serialization.Serializer;
import com.jaffa.rpc.lib.ui.AdminServer;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.jaffa.rpc.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.Executors;

@Slf4j
public class HttpAsyncResponseReceiver implements Runnable, Closeable {

    private HttpServer server;

    @Override
    public void run() {
        try {
            server = HttpServer.create(Utils.getHttpCallbackBindAddress(), 0);
            server.createContext("/response", new HttpRequestHandler());
            server.setExecutor(Executors.newFixedThreadPool(3));
            server.start();
        } catch (IOException httpServerStartupException) {
            log.error("Error during HTTP request receiver startup:", httpServerStartupException);
            throw new JaffaRpcSystemException(httpServerStartupException);
        }
        log.info("{} started", this.getClass().getSimpleName());
    }

    @Override
    public void close() {
        server.stop(2);
        log.info("HTTP async response receiver stopped");
    }

    private class HttpRequestHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange request) throws IOException {
            try {
                CallbackContainer callbackContainer = Serializer.getCtx().deserialize(ByteStreams.toByteArray(request.getRequestBody()), CallbackContainer.class);
                Class<?> callbackClass = Class.forName(callbackContainer.getListener());
                Command command = FinalizationWorker.getEventsToConsume().remove(callbackContainer.getKey());
                if (command != null) {
                    if (callbackContainer.getResult() instanceof ExceptionHolder) {
                        Method method = callbackClass.getMethod("onError", String.class, Throwable.class);
                        method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), new JaffaRpcExecutionException(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
                    } else {
                        Method method = callbackClass.getMethod("onSuccess", String.class, Class.forName(callbackContainer.getResultClass()));
                        if (Class.forName(callbackContainer.getResultClass()).equals(Void.class)) {
                            method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), null);
                        } else
                            method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                    }
                    AdminServer.addMetric(command);
                } else {
                    log.warn("Response {} already expired", callbackContainer.getKey());
                }
                String response = "OK";
                request.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = request.getResponseBody();
                os.write(response.getBytes());
                os.close();
                request.close();
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException callbackExecutionException) {
                log.error("ZMQ callback execution exception", callbackExecutionException);
                throw new JaffaRpcExecutionException(callbackExecutionException);
            }
        }
    }
}
