package com.transport.lib.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.transport.lib.entities.CallbackContainer;
import com.transport.lib.entities.ExceptionHolder;
import com.transport.lib.common.FinalizationWorker;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.zookeeper.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import zmq.ZError;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.lang.reflect.Method;

/*
    Class responsible for receiving asynchronous responses using ZeroMQ
 */
public class ZMQAsyncResponseReceiver implements Runnable, Closeable {

    private static Logger logger = LoggerFactory.getLogger(ZMQAsyncResponseReceiver.class);

    private ZMQ.Context context;
    private ZMQ.Socket socket;

    @Override
    public void run() {
        try {
            context = ZMQ.context(1);
            socket = context.socket(ZMQ.REP);
            socket.bind("tcp://" + Utils.getZeroMQCallbackBindAddress());
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
                    logger.error("General ZMQ exception", recvTerminationException);
                } catch (Exception generalExecutionException) {
                    logger.error("ZMQ response method execution exception", generalExecutionException);
                }
            }
        } catch (Exception generalZmqException) {
            logger.error("Error during callback receiver startup:", generalZmqException);
        }
        logger.info("{} terminated", this.getClass().getSimpleName());
    }

    @Override
    public void close() { Utils.closeSocketAndContext(socket, context); }
}
