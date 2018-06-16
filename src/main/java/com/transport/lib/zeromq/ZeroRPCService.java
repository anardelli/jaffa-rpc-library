package com.transport.lib.zeromq;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;

public class ZeroRPCService<T> implements Runnable {
    private T wrappedService;
    private Context context;
    private Socket socket;

    public ZeroRPCService(T wrappedService) {
        this.wrappedService = wrappedService;
    }

    public void bind(String address) {
        this.context = ZMQ.context(1);

        this.socket = context.socket(ZMQ.REP);
        this.socket.bind(address);

    }
    private Object invoke(Command command) {
        if(command.getMethodArgs() != null && command.getMethodArgs().length > 0) {
            try {
                Class[] methodArgClasses = new Class[command.getMethodArgs().length];
                for(int i = 0; i < command.getMethodArgs().length; i++){
                    methodArgClasses[i] =  Class.forName(command.getMethodArgs()[i]);
                }
                Method m = this.wrappedService.getClass().getMethod(command.getMethodName(), methodArgClasses);
                Object result = m.invoke(this.wrappedService, command.getArgs());
                if(m.getReturnType().equals(Void.TYPE)){
                    return Void.TYPE;
                }else
                    return result;
            }catch (Exception e){
                return e.getCause();
            }
        } else {
            try {
                Method m = this.wrappedService.getClass().getMethod(command.getMethodName());
                Object result = m.invoke(this.wrappedService);
                if(m.getReturnType().equals(Void.TYPE)){
                    return Void.TYPE;
                }else
                    return result;
            }catch (Exception e){
                return e.getCause();
            }
        }
    }
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                byte[] bytes = this.socket.recv();
                Kryo kryo = new Kryo();
                Input input = new Input(new ByteArrayInputStream(bytes));
                final Command command = kryo.readObject(input, Command.class);
                Object result = invoke(command);
                ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                Output output = new Output(bOutput);
                if(result instanceof Throwable){
                    StringWriter sw = new StringWriter();
                    ((Throwable)result).printStackTrace(new PrintWriter(sw));
                    kryo.writeClassAndObject(output, new ExceptionHolder(sw.toString()));
                }else {
                    kryo.writeClassAndObject(output, result);
                }
                output.close();
                this.socket.send(bOutput.toByteArray());
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        this.socket.close();
        this.context.term();
    }
}