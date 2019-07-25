package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.zookeeper.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import zmq.ZError;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.transport.lib.common.TransportService.*;

public class ZMQAsyncAndSyncRequestReceiver implements Runnable, Closeable {

    private static Logger logger = LoggerFactory.getLogger(ZMQAsyncAndSyncRequestReceiver.class);

    private static ExecutorService service = Executors.newFixedThreadPool(3);

    private ZMQ.Context context;
    private ZMQ.Socket socket;

    @Override
    public void run() {
        try {
            context = ZMQ.context(1);
            socket = context.socket(ZMQ.REP);
            socket.bind("tcp://" + Utils.getZeroMQBindAddress());
            // New Kryo instance per thread
            Kryo kryo = new Kryo();
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    byte[] bytes = socket.recv();
                    Input input = new Input(new ByteArrayInputStream(bytes));
                    final Command command = kryo.readObject(input, Command.class);
                    // It was async request, so answer with "OK" message before target message invocation
                    if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                        socket.send("OK");
                    }
                    if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                        Runnable runnable = () -> {
                                try {
                                    TransportContext.setSourceModuleId(command.getSourceModuleId());
                                    TransportContext.setSecurityTicketThreadLocal(command.getTicket());
                                    Object result = invoke(command);
                                    ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                                    Output output = new Output(bOutput);
                                    ZMQ.Context contextAsync = ZMQ.context(1);
                                    ZMQ.Socket socketAsync = contextAsync.socket(ZMQ.REQ);
                                    socketAsync.connect("tcp://" + command.getCallBackZMQ());
                                    CallbackContainer callbackContainer = new CallbackContainer();
                                    callbackContainer.setKey(command.getCallbackKey());
                                    callbackContainer.setListener(command.getCallbackClass());
                                    callbackContainer.setResult(getResult(result));
                                    Method targetMethod = getTargetMethod(command);
                                    if (primitiveToWrappers.containsKey(targetMethod.getReturnType())) {
                                        callbackContainer.setResultClass(primitiveToWrappers.get(targetMethod.getReturnType()).getName());
                                    } else {
                                        callbackContainer.setResultClass(targetMethod.getReturnType().getName());
                                    }
                                    kryo.writeObject(output, callbackContainer);
                                    output.close();
                                    socketAsync.send(bOutput.toByteArray());
                                    Utils.closeSocketAndContext(socketAsync, contextAsync);
                                } catch (Exception e) {
                                    logger.error("Error while receiving async request");
                                }
                            };
                        service.execute(runnable);
                    } else {
                        TransportContext.setSourceModuleId(command.getSourceModuleId());
                        TransportContext.setSecurityTicketThreadLocal(command.getTicket());
                        Object result = invoke(command);
                        ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                        Output output = new Output(bOutput);
                        kryo.writeClassAndObject(output, getResult(result));
                        output.close();
                        socket.send(bOutput.toByteArray());
                    }
                } catch (ZMQException | ZError.IOException recvTerminationException) {
                    logger.error("General ZMQ exception", recvTerminationException);
                } catch (Exception generalExecutionException) {
                    logger.error("ZMQ request method execution exception", generalExecutionException);
                }
            }
        } catch (Exception generalZmqException) {
            logger.error("Error during request receiver startup:", generalZmqException);
        }
        logger.info(this.getClass().getSimpleName() + " terminated");
    }

    @Override
    public void close() {
        Utils.closeSocketAndContext(socket, context);
        service.shutdownNow();
    }
}
