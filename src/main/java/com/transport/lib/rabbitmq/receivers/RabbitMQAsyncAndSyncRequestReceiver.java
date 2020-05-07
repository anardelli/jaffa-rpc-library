package com.transport.lib.rabbitmq.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.rabbitmq.client.*;
import com.transport.lib.TransportService;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.RequestContext;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.rabbitmq.RabbitMQRequestSender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.Connection;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import static com.transport.lib.TransportService.*;

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
            connection = TransportService.getConnectionFactory().createConnection();
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
                                Kryo kryo = new Kryo();
                                try {
                                    Input input = new Input(new ByteArrayInputStream(body));
                                    final Command command = kryo.readObject(input, Command.class);
                                    if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                                        Runnable runnable = () -> {
                                            try {
                                                RequestContext.setSourceModuleId(command.getSourceModuleId());
                                                RequestContext.setSecurityTicket(command.getTicket());
                                                Object result = invoke(command);
                                                ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                                                Output output = new Output(bOutput);
                                                kryo.writeObject(output, constructCallbackContainer(command, result));
                                                output.close();
                                                byte[] response = bOutput.toByteArray();
                                                Map<String, Object> headers = new HashMap<>();
                                                headers.put("communication-type", "async");
                                                AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().headers(headers).build();
                                                clientChannel.basicPublish(command.getSourceModuleId(), "client-async" + command.getSourceModuleId(), props, response);
                                                serverChannel.basicAck(envelope.getDeliveryTag(), false);
                                            } catch (ClassNotFoundException | NoSuchMethodException | IOException e) {
                                                log.error("Error while receiving async request", e);
                                                throw new TransportExecutionException(e);
                                            }
                                        };
                                        responseService.execute(runnable);
                                    } else {
                                        RequestContext.setSourceModuleId(command.getSourceModuleId());
                                        RequestContext.setSecurityTicket(command.getTicket());
                                        Object result = invoke(command);
                                        ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                                        Output output = new Output(bOutput);
                                        kryo.writeClassAndObject(output, getResult(result));
                                        output.close();
                                        byte[] response = bOutput.toByteArray();
                                        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder().correlationId(command.getRqUid()).build();
                                        clientChannel.basicPublish(command.getSourceModuleId(), "client-sync" + command.getSourceModuleId(), props, response);
                                        serverChannel.basicAck(envelope.getDeliveryTag(), false);
                                    }
                                } catch (IOException ioException) {
                                    log.error("General RabbitMQ exception", ioException);
                                    throw new TransportSystemException(ioException);
                                }
                            }
                    );
                }
            };
            serverChannel.basicConsume(RabbitMQRequestSender.SERVER, false, consumer);
        } catch (AmqpException | IOException amqpException) {
            log.error("Error during RabbitMQ request receiver startup:", amqpException);
            throw new TransportSystemException(amqpException);
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
