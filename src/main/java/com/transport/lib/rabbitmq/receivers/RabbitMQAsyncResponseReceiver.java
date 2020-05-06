package com.transport.lib.rabbitmq.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.rabbitmq.client.*;
import com.transport.lib.TransportService;
import com.transport.lib.common.FinalizationWorker;
import com.transport.lib.entities.CallbackContainer;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.ExceptionHolder;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.ui.AdminServer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.Connection;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RabbitMQAsyncResponseReceiver implements Runnable, Closeable {
    private static final String EXCHANGE_NAME = TransportService.getRequiredOption("module.id");
    private static final String CLIENT_ROUTING_KEY = "client-async";
    private static final String CLIENT_ASYNC_QUEUE_NAME = "client-async";
    private Connection connection;
    private Channel clientChannel;

    @Override
    public void run() {
        try {
            connection = TransportService.getConnectionFactory().createConnection();
            clientChannel = connection.createChannel(false);
            clientChannel.queueBind(CLIENT_ASYNC_QUEUE_NAME, EXCHANGE_NAME, CLIENT_ROUTING_KEY);
            Consumer consumer = new DefaultConsumer(clientChannel) {
                @Override
                public void handleDelivery(
                        String consumerTag,
                        Envelope envelope,
                        AMQP.BasicProperties properties,
                        final byte[] body) {
                    if(properties.getHeaders() == null) return;
                    Object type = properties.getHeaders().get("communication-type");
                    if (type == null || !"async".equals(String.valueOf(type))) return;
                    Kryo kryo = new Kryo();
                    try {
                        Input input = new Input(new ByteArrayInputStream(body));
                        CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                        Class<?> callbackClass = Class.forName(callbackContainer.getListener());
                        Command command = FinalizationWorker.eventsToConsume.remove(callbackContainer.getKey());
                        if (command != null) {
                            if (callbackContainer.getResult() instanceof ExceptionHolder) {
                                java.lang.reflect.Method method = callbackClass.getMethod("onError", String.class, Throwable.class);
                                method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), new TransportExecutionException(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
                            } else {
                                Method method = callbackClass.getMethod("onSuccess", String.class, Class.forName(callbackContainer.getResultClass()));
                                if (Class.forName(callbackContainer.getResultClass()).equals(Void.class)) {
                                    method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), null);
                                } else
                                    method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                            }
                            AdminServer.addMetric(command);
                            clientChannel.basicAck(envelope.getDeliveryTag(), false);
                        } else {
                            log.warn("Response {} already expired", callbackContainer.getKey());
                        }
                    } catch (IOException ioException) {
                        log.error("General RabbitMQ exception", ioException);
                        throw new TransportSystemException(ioException);
                    } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException callbackExecutionException) {
                        log.error("RabbitMQ callback execution exception", callbackExecutionException);
                        throw new TransportExecutionException(callbackExecutionException);
                    }
                }
            };
            clientChannel.basicConsume(CLIENT_ROUTING_KEY, false, consumer);
        } catch (AmqpException | IOException ioException) {
            log.error("Error during RabbitMQ response receiver startup:", ioException);
            throw new TransportSystemException(ioException);
        }
    }

    @Override
    public void close() {
        try {
            clientChannel.close();
        } catch (IOException | TimeoutException ignore) {
        }
        connection.close();
    }
}
