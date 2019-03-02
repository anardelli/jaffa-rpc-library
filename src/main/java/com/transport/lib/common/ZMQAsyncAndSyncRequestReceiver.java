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

import static com.transport.lib.common.TransportService.*;

@SuppressWarnings("all")
public class ZMQAsyncAndSyncRequestReceiver implements Runnable, Closeable {

    private static Logger logger = LoggerFactory.getLogger(ZMQAsyncAndSyncRequestReceiver.class);

    private ZMQ.Context context;
    private ZMQ.Socket socket;

    @Override
    public void run() {
        try{
            context = ZMQ.context(1);
            socket = context.socket(ZMQ.REP);
            socket.bind("tcp://" + Utils.getZeroMQBindAddress());
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    byte[] bytes = socket.recv();
                    Kryo kryo = new Kryo();
                    Input input = new Input(new ByteArrayInputStream(bytes));
                    Command command = kryo.readObject(input, Command.class);
                    if(command.getCallbackKey() != null && command.getCallbackClass() != null) {
                        socket.send("OK");
                    }
                    TransportContext.setSourceModuleId(command.getSourceModuleId());
                    TransportContext.setSecurityTicketThreadLocal(command.getTicket());
                    Object result = null;
                    try{
                        result = invoke(command);
                    }catch (Exception executionException){
                        logger.error("Target method execution exception", executionException);
                    }
                    ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                    Output output = new Output(bOutput);
                    if(command.getCallbackKey() != null && command.getCallbackClass() != null){
                        ZMQ.Context contextAsync = ZMQ.context(1);
                        ZMQ.Socket socketAsync = contextAsync.socket(ZMQ.REQ);
                        socketAsync.connect("tcp://" + command.getCallBackZMQ());
                        CallbackContainer callbackContainer = new CallbackContainer();
                        callbackContainer.setKey(command.getCallbackKey());
                        callbackContainer.setListener(command.getCallbackClass());
                        callbackContainer.setResult(getResult(result));
                        Method targetMethod = getTargetMethod(command);
                        if(map.containsKey(targetMethod.getReturnType())){
                            callbackContainer.setResultClass(map.get(targetMethod.getReturnType()).getName());
                        }else{
                            callbackContainer.setResultClass(targetMethod.getReturnType().getName());
                        }
                        kryo.writeObject(output, callbackContainer);
                        output.close();
                        socketAsync.send(bOutput.toByteArray());
                        Utils.closeSocketAndContext(socketAsync, contextAsync);
                    }else{
                        kryo.writeClassAndObject(output, getResult(result));
                        output.close();
                        socket.send(bOutput.toByteArray());
                    }
                } catch (ZMQException | ZError.IOException recvTerminationException) {
                } catch (Exception generalExecutionException) {
                    logger.error("ZMQ request method execution exception", generalExecutionException);
                }
            }
        }catch (Exception generalZmqException){
            logger.error("Error during request receiver startup:", generalZmqException);
        }
        logger.info(this.getClass().getSimpleName() + " terminated");
    }

    @Override
    public void close(){
        Utils.closeSocketAndContext(socket, context);
    }
}
