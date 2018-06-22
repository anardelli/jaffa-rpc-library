package com.transport.lib.zeromq;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.zookeeper.ZKUtils;
import org.zeromq.ZMQ;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class Request<T> implements RequestInterface<T>{

    private int timeout = -1;
    private String moduleId;

    private Command command;

    public Request(Command command){
        this.command = command;
    }

    public Request<T> withTimeout(int timeout){
        this.timeout = timeout;
        return this;
    }

    public Request<T> onModule(String moduleId){
        this.moduleId = moduleId;
        return this;
    }

    @SuppressWarnings("unchecked")
    public T execute(){
        String address = "tcp://" + ZKUtils.getHostForService(command.getServiceClass(), moduleId);
        ZMQ.Context context =  ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.REQ);
        socket.connect(address);
        Kryo kryo = new Kryo();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        socket.send(out.toByteArray(), 0);
        if(timeout != -1) {
            socket.setReceiveTimeOut(timeout);
        }
        byte[] response = socket.recv(0);
        if(response == null) {
            socket.close();
            if(!context.isClosed()){
                context.close();
            }
            throw new RuntimeException("Transport execution timeout");
        }
        Input input = new Input(new ByteArrayInputStream(response));
        Object result = kryo.readClassAndObject(input);
        input.close();
        if(result != null){
            if(result instanceof ExceptionHolder)
                throw new RuntimeException(((ExceptionHolder) result).getStackTrace());
        }

        return (T)result;
    }
}
