package com.jaffa.rpc.lib.zeromq.receivers;

import com.jaffa.rpc.lib.common.FinalizationWorker;
import com.jaffa.rpc.lib.entities.CallbackContainer;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.entities.ExceptionHolder;
import com.jaffa.rpc.lib.exception.JaffaRpcExecutionException;
import com.jaffa.rpc.lib.exception.JaffaRpcSystemException;
import com.jaffa.rpc.lib.serialization.Serializer;
import com.jaffa.rpc.lib.ui.AdminServer;
import com.jaffa.rpc.lib.zeromq.CurveUtils;
import com.jaffa.rpc.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.*;
import zmq.ZError;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;

@Slf4j
public class ZMQAsyncResponseReceiver implements Runnable, Closeable {

    private ZContext context;
    private ZAuth auth;

    @Override
    public void run() {
        ZMQ.Socket socket;
        try {
            context = new ZContext(10);
            context.setLinger(0);
            if (Boolean.parseBoolean(System.getProperty("jaffa.rpc.protocol.zmq.curve.enabled", "false"))) {
                auth = new ZAuth(context);
                auth.setVerbose(true);
                auth.configureCurve(Utils.getRequiredOption("jaffa.rpc.protocol.zmq.client.dir"));
            }
            socket = context.createSocket(SocketType.REP);
            if (Boolean.parseBoolean(System.getProperty("jaffa.rpc.protocol.zmq.curve.enabled", "false"))) {
                socket.setZAPDomain("global".getBytes());
                socket.setCurveServer(true);
                socket.setCurvePublicKey(CurveUtils.getServerPublicKey().getBytes());
                socket.setCurveSecretKey(CurveUtils.getServerSecretKey().getBytes());
            }
            socket.bind("tcp://" + Utils.getZeroMQCallbackBindAddress());
        } catch (UnknownHostException zmqStartupException) {
            log.error("Error during ZeroMQ response receiver startup:", zmqStartupException);
            throw new JaffaRpcSystemException(zmqStartupException);
        }
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] bytes = socket.recv();
                CallbackContainer callbackContainer = Serializer.getCtx().deserialize(bytes, CallbackContainer.class);
                Class<?> callbackClass = Class.forName(callbackContainer.getListener());
                Command command = FinalizationWorker.getEventsToConsume().remove(callbackContainer.getKey());
                if (command != null) {
                    if (callbackContainer.getResult() instanceof ExceptionHolder) {
                        Method method = callbackClass.getMethod("onError", String.class, Throwable.class);
                        method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), new JaffaRpcExecutionException(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
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
                if (!recvTerminationException.getMessage().contains("Errno 4") && !recvTerminationException.getMessage().contains("156384765")) {
                    log.error("General ZMQ exception", recvTerminationException);
                    throw new JaffaRpcSystemException(recvTerminationException);
                }
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException | ClassNotFoundException | NoSuchMethodException callbackExecutionException) {
                log.error("ZMQ callback execution exception", callbackExecutionException);
                throw new JaffaRpcExecutionException(callbackExecutionException);
            }
        }
        log.info("{} terminated", this.getClass().getSimpleName());
    }

    @Override
    public void close() {
        if (Boolean.parseBoolean(System.getProperty("jaffa.rpc.protocol.zmq.curve.enabled", "false"))) {
            try {
                auth.close();
            } catch (IOException ioException) {
                log.error("Error while closing ZeroMQ context", ioException);
            }
        } else {
            context.close();
        }
    }
}
