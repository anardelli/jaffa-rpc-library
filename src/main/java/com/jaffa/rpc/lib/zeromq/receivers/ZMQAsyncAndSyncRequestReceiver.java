package com.jaffa.rpc.lib.zeromq.receivers;

import com.jaffa.rpc.lib.JaffaService;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.entities.RequestContext;
import com.jaffa.rpc.lib.exception.TransportExecutionException;
import com.jaffa.rpc.lib.exception.TransportSystemException;
import com.jaffa.rpc.lib.serialization.Serializer;
import com.jaffa.rpc.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import zmq.ZError;

import java.io.Closeable;
import java.net.UnknownHostException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class ZMQAsyncAndSyncRequestReceiver implements Runnable, Closeable {

    private static final ExecutorService service = Executors.newFixedThreadPool(3);

    private ZMQ.Context context;
    private ZMQ.Socket socket;

    @Override
    public void run() {
        try {
            context = ZMQ.context(10);
            socket = context.socket(SocketType.REP);
            socket.bind("tcp://" + Utils.getZeroMQBindAddress());
        } catch (UnknownHostException zmqStartupException) {
            log.error("Error during ZeroMQ request receiver startup:", zmqStartupException);
            throw new TransportSystemException(zmqStartupException);
        }
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] bytes = socket.recv();
                final Command command = Serializer.getCtx().deserialize(bytes, Command.class);
                if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                    socket.send("OK");
                }
                if (command.getCallbackKey() != null && command.getCallbackClass() != null) {
                    Runnable runnable = () -> {
                        try {
                            RequestContext.setSourceModuleId(command.getSourceModuleId());
                            RequestContext.setSecurityTicket(command.getTicket());
                            Object result = JaffaService.invoke(command);
                            byte[] serializedResponse = Serializer.getCtx().serialize(JaffaService.constructCallbackContainer(command, result));
                            ZMQ.Socket socketAsync = context.socket(SocketType.REQ);
                            socketAsync.connect("tcp://" + command.getCallBackZMQ());
                            socketAsync.send(serializedResponse);
                            socketAsync.close();
                        } catch (ClassNotFoundException | NoSuchMethodException e) {
                            log.error("Error while receiving async request", e);
                            throw new TransportExecutionException(e);
                        }
                    };
                    service.execute(runnable);
                } else {
                    RequestContext.setSourceModuleId(command.getSourceModuleId());
                    RequestContext.setSecurityTicket(command.getTicket());
                    Object result = JaffaService.invoke(command);
                    byte[] serializedResponse = Serializer.getCtx().serializeWithClass(JaffaService.getResult(result));
                    socket.send(serializedResponse);
                }
            } catch (ZMQException | ZError.IOException recvTerminationException) {
                if (!recvTerminationException.getMessage().contains("156384765")) {
                    log.error("General ZMQ exception", recvTerminationException);
                    throw new TransportSystemException(recvTerminationException);
                }
            }
        }
        log.info("{} terminated", this.getClass().getSimpleName());
    }

    @Override
    public void close() {
        Utils.closeSocketAndContext(socket, context);
        service.shutdownNow();
    }
}
