package com.transport.lib.request;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.callbacks.Callback;
import com.transport.lib.common.FinalizationWorker;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.ExceptionHolder;
import com.transport.lib.entities.Protocol;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportExecutionTimeoutException;
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.http.HttpRequestSender;
import com.transport.lib.kafka.KafkaRequestSender;
import com.transport.lib.rabbitmq.RabbitMQRequestSender;
import com.transport.lib.ui.AdminServer;
import com.transport.lib.zeromq.ZeroMqRequestSender;
import com.transport.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

@Slf4j
public class RequestImpl<T> implements Request<T> {

    private final Command command;
    private final Kryo kryo = new Kryo();
    private final Sender sender;
    private long timeout = -1;
    private String moduleId;

    public RequestImpl(Command command) {
        this.command = command;
        Protocol protocol = Utils.getTransportProtocol();
        switch (protocol) {
            case ZMQ:
                sender = new ZeroMqRequestSender();
                break;
            case KAFKA:
                sender = new KafkaRequestSender();
                break;
            case HTTP:
                sender = new HttpRequestSender();
                break;
            case RABBIT:
                sender = new RabbitMQRequestSender();
                break;
            default:
                throw new TransportSystemException(TransportSystemException.NO_PROTOCOL_DEFINED);
        }
    }

    public RequestImpl<T> withTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public RequestImpl<T> onModule(String moduleId) {
        this.moduleId = moduleId;
        return this;
    }

    private void initSender() {
        sender.setCommand(command);
        sender.setModuleId(moduleId);
        sender.setTimeout(timeout);
    }

    @SuppressWarnings("unchecked")
    public T executeSync() {
        initSender();
        command.setRequestTime(System.currentTimeMillis());
        command.setLocalRequestTime(System.nanoTime());
        byte[] out = marshallCommand(command);
        byte[] response = sender.executeSync(out);
        if (response == null) {
            throw new TransportExecutionTimeoutException();
        }
        Input input = new Input(new ByteArrayInputStream(response));
        Object result = kryo.readClassAndObject(input);
        input.close();
        AdminServer.addMetric(command);
        if (result instanceof ExceptionHolder)
            throw new TransportExecutionException(((ExceptionHolder) result).getStackTrace());
        return (T) result;
    }

    private byte[] marshallCommand(Command command) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        return out.toByteArray();
    }

    public void executeAsync(String key, Class<? extends Callback<T>> listener) {
        initSender();
        command.setCallbackClass(listener.getName());
        command.setCallbackKey(key);
        command.setRequestTime(System.currentTimeMillis());
        command.setLocalRequestTime(System.nanoTime());
        command.setAsyncExpireTime(System.currentTimeMillis() + (timeout != -1 ? timeout : 1000 * 60 * 60));
        log.debug("Async command {} added to finalization queue", command);
        FinalizationWorker.getEventsToConsume().put(command.getCallbackKey(), command);
        sender.executeAsync(marshallCommand(command));
    }
}
