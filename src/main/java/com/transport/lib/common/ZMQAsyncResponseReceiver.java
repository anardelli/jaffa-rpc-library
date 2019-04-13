package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.transport.lib.zookeeper.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import zmq.ZError;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.lang.reflect.Method;

@SuppressWarnings("all")
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

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    byte[] bytes = socket.recv();
                    Kryo kryo = new Kryo();
                    Input input = new Input(new ByteArrayInputStream(bytes));
                    CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                    Class callbackClass = Class.forName(callbackContainer.getListener());
                    if (FinalizationWorker.eventsToConsume.remove(callbackContainer.getKey()) != null) {
                        if (callbackContainer.getResult() instanceof ExceptionHolder) {
                            Method method = callbackClass.getMethod("callBackError", String.class, Throwable.class);
                            method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), new Throwable(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
                        } else {
                            Method method = callbackClass.getMethod("callBack", String.class, Class.forName(callbackContainer.getResultClass()));
                            if (Class.forName(callbackContainer.getResultClass()).equals(Void.class)) {
                                method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), null);
                            } else
                                method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                        }
                    } else {
                        logger.warn("Response " + callbackContainer.getKey() + " already expired");
                    }
                } catch (ZMQException | ZError.IOException recvTerminationException) {
                } catch (Exception generalExecutionException) {
                    logger.error("ZMQ response method execution exception", generalExecutionException);
                }
            }
        } catch (Exception generalZmqException) {
            logger.error("Error during callback receiver startup:", generalZmqException);
        }
        logger.info(this.getClass().getSimpleName() + " terminated");
    }

    @Override
    public void close() {
        Utils.closeSocketAndContext(socket, context);
    }
}
