package com.transport.lib.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.transport.lib.common.FinalizationWorker;
import com.transport.lib.entities.CallbackContainer;
import com.transport.lib.entities.ExceptionHolder;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.zookeeper.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import zmq.ZError;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;

/*
    Class responsible for receiving asynchronous responses using ZeroMQ
 */
public class ZMQAsyncResponseReceiver implements Runnable, Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ZMQAsyncResponseReceiver.class);

    private ZMQ.Context context;
    private ZMQ.Socket socket;

    @Override
    public void run() {
        try {
            context = ZMQ.context(1);
            socket = context.socket(ZMQ.REP);
            socket.bind("tcp://" + Utils.getZeroMQCallbackBindAddress());
        } catch (UnknownHostException zmqStartupException) {
            logger.error("Error during ZeroMQ response receiver startup:", zmqStartupException);
            throw new TransportSystemException(zmqStartupException);
        }
        // New Kryo instance per thread
        Kryo kryo = new Kryo();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                // Receive raw bytes
                byte[] bytes = socket.recv();
                Input input = new Input(new ByteArrayInputStream(bytes));
                // Unmarshal bytes to CallbackContainer
                CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                // Get target callback class
                Class<?> callbackClass = Class.forName(callbackContainer.getListener());
                // If request is still valid
                if (FinalizationWorker.eventsToConsume.remove(callbackContainer.getKey()) != null) {
                    // Send result to callback by invoking appropriate method
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
                } else {
                    logger.warn("Response {} already expired", callbackContainer.getKey());
                }
            } catch (ZMQException | ZError.IOException recvTerminationException) {
                if(!recvTerminationException.getMessage().contains("156384765")) {
                    logger.error("General ZMQ exception", recvTerminationException);
                    throw new TransportSystemException(recvTerminationException);
                }
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException callbackExecutionException) {
                logger.error("ZMQ callback execution exception", callbackExecutionException);
                throw new TransportExecutionException(callbackExecutionException);
            }
        }

        logger.info("{} terminated", this.getClass().getSimpleName());
    }

    @Override
    public void close() {
        Utils.closeSocketAndContext(socket, context);
    }
}
