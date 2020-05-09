package com.jaffa.rpc.lib.request;

import com.jaffa.rpc.lib.callbacks.Callback;
import com.jaffa.rpc.lib.common.FinalizationWorker;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.entities.ExceptionHolder;
import com.jaffa.rpc.lib.entities.Protocol;
import com.jaffa.rpc.lib.exception.TransportExecutionException;
import com.jaffa.rpc.lib.exception.TransportExecutionTimeoutException;
import com.jaffa.rpc.lib.exception.TransportSystemException;
import com.jaffa.rpc.lib.http.HttpRequestSender;
import com.jaffa.rpc.lib.kafka.KafkaRequestSender;
import com.jaffa.rpc.lib.rabbitmq.RabbitMQRequestSender;
import com.jaffa.rpc.lib.serialization.Serializer;
import com.jaffa.rpc.lib.ui.AdminServer;
import com.jaffa.rpc.lib.zeromq.ZeroMqRequestSender;
import com.jaffa.rpc.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RequestImpl<T> implements Request<T> {

    private final Command command;
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
        byte[] out = Serializer.getCtx().serialize(command);
        byte[] response = sender.executeSync(out);
        if (response == null) {
            throw new TransportExecutionTimeoutException();
        }
        Object result = Serializer.getCtx().deserializeWithClass(response);
        AdminServer.addMetric(command);
        if (result instanceof ExceptionHolder)
            throw new TransportExecutionException(((ExceptionHolder) result).getStackTrace());
        return (T) result;
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
        sender.executeAsync(Serializer.getCtx().serialize(command));
    }
}
