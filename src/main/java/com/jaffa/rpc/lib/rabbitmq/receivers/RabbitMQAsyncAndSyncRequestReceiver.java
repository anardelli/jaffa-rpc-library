package com.jaffa.rpc.lib.rabbitmq.receivers;

import com.jaffa.rpc.lib.JaffaService;
import com.jaffa.rpc.lib.common.RequestInvoker;
import com.jaffa.rpc.lib.entities.CallbackContainer;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.entities.RequestContext;
import com.jaffa.rpc.lib.exception.JaffaRpcExecutionException;
import com.jaffa.rpc.lib.exception.JaffaRpcSystemException;
import com.jaffa.rpc.lib.rabbitmq.RabbitMQRequestSender;
import com.jaffa.rpc.lib.serialization.Serializer;
import com.rabbitmq.client.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.Connection;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RabbitMQAsyncAndSyncRequestReceiver implements Runnable, Closeable {

    private static final ExecutorService responseService = Executors.newFixedThreadPool(3);
    private static final ExecutorService requestService = Executors.newFixedThreadPool(3);
    private Connection connection;
    private Channel serverChannel;
    private Channel clientChannel;

    @Override
    public void run() {
        try {
            connection = JaffaService.getConnectionFactory().createConnection();
            serverChannel = connection.createChannel(false);
            clientChannel = connection.createChannel(false);
            serverChannel.queueBind(RabbitMQRequestSender.SERVER, RabbitMQRequestSender.EXCHANGE_NAME, RabbitMQRequestSender.SERVER);
            Consumer consumer = new DefaultConsumer(serverChannel) {
                @Override
                public void handleDelivery(
                        String consumerTag,
                        Envelope envelope,
                        AMQP.BasicProperties properties,
                        final byte[] body) {
                    requestService.execute(() -> {
                                try {
                                    final com.jaffa.rpc.lib.entities.Command command = Serializer.getCtx().deserialize(body, Command.class);
                                    if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                                        Runnable runnable = () -> {
                                            try {
                                                RequestContext.setSourceModuleId(command.getSourceModuleId());
                                                RequestContext.setSecurityTicket(command.getTicket());
                                                Object result = RequestInvoker.invoke(command);
                                                CallbackContainer callbackContainer = RequestInvoker.constructCallbackContainer(command, result);
                                                byte[] response = Serializer.getCtx().serialize(callbackContainer);
                                                Map<String, Object> headers = new HashMap<>();
                                                headers.put("communication-type", "async");
                                                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().headers(headers).build();
                                                clientChannel.basicPublish(command.getSourceModuleId(), command.getSourceModuleId() + "-client-async", props, response);
                                                serverChannel.basicAck(envelope.getDeliveryTag(), false);
                                            } catch (ClassNotFoundException | NoSuchMethodException | IOException e) {
                                                log.error("Error while receiving async request", e);
                                                throw new JaffaRpcExecutionException(e);
                                            }
                                        };
                                        responseService.execute(runnable);
                                    } else {
                                        RequestContext.setSourceModuleId(command.getSourceModuleId());
                                        RequestContext.setSecurityTicket(command.getTicket());
                                        Object result = RequestInvoker.invoke(command);
                                        byte[] response = Serializer.getCtx().serializeWithClass(RequestInvoker.getResult(result));
                                        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(command.getRqUid()).build();
                                        clientChannel.basicPublish(command.getSourceModuleId(), command.getSourceModuleId() + "-client-sync", props, response);
                                        serverChannel.basicAck(envelope.getDeliveryTag(), false);
                                    }
                                } catch (IOException ioException) {
                                    log.error("General RabbitMQ exception", ioException);
                                    throw new JaffaRpcSystemException(ioException);
                                }
                            }
                    );
                }
            };
            serverChannel.basicConsume(RabbitMQRequestSender.SERVER, false, consumer);
        } catch (AmqpException | IOException amqpException) {
            log.error("Error during RabbitMQ request receiver startup:", amqpException);
            throw new JaffaRpcSystemException(amqpException);
        }
        log.info("{} terminated", this.getClass().getSimpleName());
    }

    @Override
    public void close() {
        try {
            serverChannel.close();
            clientChannel.close();
        } catch (IOException | TimeoutException ignore) {
        }
        connection.close();
        responseService.shutdownNow();
        requestService.shutdownNow();
    }
}
