package com.transport.lib.zeromq.receivers;

import com.transport.lib.common.FinalizationWorker;
import com.transport.lib.entities.CallbackContainer;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.ExceptionHolder;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.serialization.KryoPoolSerializer;
import com.transport.lib.ui.AdminServer;
import com.transport.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import zmq.ZError;

import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;

@Slf4j
public class ZMQAsyncResponseReceiver implements Runnable, Closeable {

    private ZMQ.Context context;
    private ZMQ.Socket socket;

    @Override
    public void run() {
        try {
            context = ZMQ.context(10);
            socket = context.socket(SocketType.REP);
            socket.bind("tcp://" + Utils.getZeroMQCallbackBindAddress());
        } catch (UnknownHostException zmqStartupException) {
            log.error("Error during ZeroMQ response receiver startup:", zmqStartupException);
            throw new TransportSystemException(zmqStartupException);
        }
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] bytes = socket.recv();
                CallbackContainer callbackContainer = KryoPoolSerializer.serializer.deserialize(bytes, CallbackContainer.class);
                Class<?> callbackClass = Class.forName(callbackContainer.getListener());
                Command command = FinalizationWorker.getEventsToConsume().remove(callbackContainer.getKey());
                if (command != null) {
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
                    AdminServer.addMetric(command);
                } else {
                    log.warn("Response {} already expired", callbackContainer.getKey());
                }
            } catch (ZMQException | ZError.IOException recvTerminationException) {
                if (!recvTerminationException.getMessage().contains("156384765")) {
                    log.error("General ZMQ exception", recvTerminationException);
                    throw new TransportSystemException(recvTerminationException);
                }
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException callbackExecutionException) {
                log.error("ZMQ callback execution exception", callbackExecutionException);
                throw new TransportExecutionException(callbackExecutionException);
            }
        }
        log.info("{} terminated", this.getClass().getSimpleName());
    }

    @Override
    public void close() {
        Utils.closeSocketAndContext(socket, context);
    }
}
