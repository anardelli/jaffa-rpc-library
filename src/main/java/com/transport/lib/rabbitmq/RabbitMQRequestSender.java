package com.transport.lib.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.transport.lib.entities.Protocol;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.connection.Connection;
import com.transport.lib.TransportService;
import com.transport.lib.request.Sender;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Slf4j
public class RabbitMQRequestSender extends Sender {

    private static Connection connection;
    private static Channel clientChannel;

    public static void init(){
        connection = TransportService.getConnectionFactory().createConnection();
        clientChannel = connection.createChannel(false);
    }

    @Override
    public byte[] executeSync(byte[] message) {
        try {
            if (moduleId != null && !moduleId.isEmpty()) {
                clientChannel.basicPublish(command.getSourceModuleId(), "client", null, message);
            }else{
                String transportInterface = command.getServiceClass();
                String serviceInterface = transportInterface.replaceFirst("Transport", "");
                String moduleId = Utils.getModuleForService(serviceInterface, Protocol.RABBIT);
                clientChannel.basicPublish(moduleId, "client", null, message);
            }
            long start = System.currentTimeMillis();
            while (!((timeout != -1 && System.currentTimeMillis() - start > timeout) || (System.currentTimeMillis() - start > (1000 * 60 * 60)))) {
                GetResponse response = clientChannel.basicGet("client", false);
                if(command.getRqUid().equals(response.getProps().getCorrelationId())){
                    clientChannel.basicAck(response.getEnvelope().getDeliveryTag(), false);
                    return response.getBody();
                }
            }
        }catch (IOException ioException){
            log.error("Error while sending sync RabbitMQ request", ioException);
            throw new TransportExecutionException(ioException);
        }
        return null;
    }

    @Override
    public void executeAsync(byte[] message) {
        try {
            if (moduleId != null && !moduleId.isEmpty()) {
                clientChannel.basicPublish(command.getSourceModuleId(), "client", null, message);
            }else{
                String transportInterface = command.getServiceClass();
                String serviceInterface = transportInterface.replaceFirst("Transport", "");
                String moduleId = Utils.getModuleForService(serviceInterface, Protocol.RABBIT);
                clientChannel.basicPublish(moduleId, "client", null, message);
            }
        }catch (IOException e){
            log.error("Error while sending async RabbitMQ request", e);
            throw new TransportExecutionException(e);
        }
    }

    public static void close(){
        try {
            clientChannel.close();
        } catch (IOException | TimeoutException ignore) {
        }
        connection.close();
    }
}