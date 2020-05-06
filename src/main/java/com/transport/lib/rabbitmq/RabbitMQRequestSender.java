package com.transport.lib.rabbitmq;

import com.rabbitmq.client.*;
import com.transport.lib.TransportService;
import com.transport.lib.entities.Protocol;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.request.Sender;
import com.transport.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.AmqpException;
import org.springframework.amqp.rabbit.connection.Connection;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class RabbitMQRequestSender extends Sender {

    private static Connection connection;
    private static Channel clientChannel;
    private static final String EXCHANGE_NAME = TransportService.getRequiredOption("module.id");
    private static final String CLIENT_ROUTING_KEY = "client-sync" + TransportService.getRequiredOption("module.id");
    private static final String CLIENT_SYNC_QUEUE_NAME = "client-sync" + TransportService.getRequiredOption("module.id");
    private static final Map<String, Callback> requests = new ConcurrentHashMap<>();
    public static void init() {
        try {
            connection = TransportService.getConnectionFactory().createConnection();
            clientChannel = connection.createChannel(false);
            clientChannel.queueBind(CLIENT_SYNC_QUEUE_NAME, EXCHANGE_NAME, CLIENT_ROUTING_KEY);
            Consumer consumer = new DefaultConsumer(clientChannel) {
                @Override
                public void handleDelivery(
                        String consumerTag,
                        Envelope envelope,
                        AMQP.BasicProperties properties,
                        final byte[] body) throws IOException {
                    if(properties != null && properties.getCorrelationId()!= null){
                        Callback callback = requests.remove(properties.getCorrelationId());
                        if(callback != null) {
                            callback.call(body);
                            clientChannel.basicAck(envelope.getDeliveryTag(), false);
                        }
                    }
                }
            };
            clientChannel.basicConsume(CLIENT_ROUTING_KEY, false, consumer);
        } catch (AmqpException | IOException ioException) {
            log.error("Error during RabbitMQ response receiver startup:", ioException);
            throw new TransportSystemException(ioException);
        }
    }

    public static void close() {
        try {
            clientChannel.close();
        } catch (IOException | TimeoutException ignore) {
        }
        connection.close();
    }

    @Override
    public byte[] executeSync(byte[] message) {
        try {
            final AtomicReference<byte[]> atomicReference = new AtomicReference<>();
            requests.put(command.getRqUid(), atomicReference::set);
            if (moduleId != null && !moduleId.isEmpty()) {
                clientChannel.basicPublish(command.getSourceModuleId(), "server" + moduleId, null, message);
            } else {
                String transportInterface = command.getServiceClass();
                String serviceInterface = transportInterface.replaceFirst("Transport", "");
                String moduleId = Utils.getModuleForService(serviceInterface, Protocol.RABBIT);
                clientChannel.basicPublish(moduleId, "server" + moduleId, null, message);
            }
            long start = System.currentTimeMillis();
            while (!((timeout != -1 && System.currentTimeMillis() - start > timeout) || (System.currentTimeMillis() - start > (1000 * 60 * 60)))) {
                byte[] result = atomicReference.get();
                if(result != null){
                    return result;
                }
            }
            requests.remove(command.getRqUid());
        } catch (IOException ioException) {
            log.error("Error while sending sync RabbitMQ request", ioException);
            throw new TransportExecutionException(ioException);
        }
        return null;
    }

    @Override
    public void executeAsync(byte[] message) {
        try {
            if (moduleId != null && !moduleId.isEmpty()) {
                clientChannel.basicPublish(command.getSourceModuleId(), "server" + moduleId, null, message);
            } else {
                String transportInterface = command.getServiceClass();
                String serviceInterface = transportInterface.replaceFirst("Transport", "");
                String moduleId = Utils.getModuleForService(serviceInterface, Protocol.RABBIT);
                clientChannel.basicPublish(moduleId, "server" + moduleId, null, message);
            }
        } catch (IOException e) {
            log.error("Error while sending async RabbitMQ request", e);
            throw new TransportExecutionException(e);
        }
    }

    private abstract interface Callback {
        void call(byte[] body);
    }
}
