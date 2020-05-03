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
import com.transport.lib.ui.AdminServer;
import com.transport.lib.zeromq.ZeroMqRequestSender;
import com.transport.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/*
    Class responsible for making synchronous and asynchronous requests
 */
@Slf4j
public class RequestImpl<T> implements Request<T> {

    // Target command
    private final Command command;
    // New Kryo instance per thread
    private final Kryo kryo = new Kryo();
    private final Sender sender;
    // Time period in milliseconds during which we wait for answer from server
    private long timeout = -1;
    // Target module.id
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
            default:
                throw new TransportSystemException(TransportSystemException.NO_PROTOCOL_DEFINED);
        }
    }

    /*
        Setter for user-provided timeout
     */
    public RequestImpl<T> withTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    /*
        Setter for user-provided server module.id
     */
    public RequestImpl<T> onModule(String moduleId) {
        this.moduleId = moduleId;
        return this;
    }

    private void initSender() {
        sender.setCommand(command);
        sender.setModuleId(moduleId);
        sender.setTimeout(timeout);
    }

    /*
        Responsible for making synchronous request and waiting for answer using Kafka or ZeroMQ
        @SuppressWarnings because Kryo returns raw Object
     */
    @SuppressWarnings("unchecked")
    public T executeSync() {
        initSender();
        command.setRequestTime(System.currentTimeMillis());
        command.setLocalRequestTime(System.nanoTime());
        // Serialize command-request
        byte[] out = marshallCommand(command);
        // Response from server, if null - transport timeout occurred
        byte[] response = sender.executeSync(out);
        // Response could be null ONLY of timeout occurred, otherwise it is object or void or ExceptionHolder
        if (response == null) {
            throw new TransportExecutionTimeoutException();
        }
        Input input = new Input(new ByteArrayInputStream(response));
        Object result = kryo.readClassAndObject(input);
        input.close();
        AdminServer.addMetric(command);
        // Server returned ExceptionHolder - exception occurred on server side
        if (result instanceof ExceptionHolder)
            throw new TransportExecutionException(((ExceptionHolder) result).getStackTrace());
        return (T) result;
    }

    private byte[] marshallCommand(Command command) {
        // Marshall Command using Kryo
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        return out.toByteArray();
    }

    /*
        Responsible for making asynchronous request using Kafka or ZeroMQ
     */
    public void executeAsync(String key, Class<? extends Callback<T>> listener) {
        initSender();
        // Set Callback class name
        command.setCallbackClass(listener.getName());
        // Set user-provided unique callback key
        command.setCallbackKey(key);
        command.setRequestTime(System.currentTimeMillis());
        command.setLocalRequestTime(System.nanoTime());
        // Add command to background finalization thread
        // that will throw "Transport execution timeout" on callback class after timeout expiration or 60 minutes if timeout was not set
        command.setAsyncExpireTime(System.currentTimeMillis() + (timeout != -1 ? timeout : 1000 * 60 * 60));
        log.debug("Async command {} added to finalization queue", command);
        // Add Command to finalization queue before request execution to avoid race between adding to queue and receiving response
        FinalizationWorker.eventsToConsume.put(command.getCallbackKey(), command);
        // Send Request using Kafka or ZeroMQ
        sender.executeAsync(marshallCommand(command));
    }
}
