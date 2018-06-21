package com.transport.lib.zeromq;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.zookeeper.ZKUtils;
import org.reflections.Reflections;
import org.springframework.core.annotation.AnnotationUtils;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Set;

public class ZeroRPCService implements Runnable {
    private HashMap<Class, Object> wrappedServices = new HashMap<>();
    private Context context;
    private Socket socket;

    ZeroRPCService() {

        String serviceRoot = System.getProperty("service.root");
        if(serviceRoot == null) throw new IllegalArgumentException("Property service.root was not set");

        String zooConnection = System.getProperty("zookeeper.connection");
        if(zooConnection == null) throw new IllegalArgumentException("Property zookeeper.connection was not set");

        ZKUtils.connect(zooConnection);
        try{
            Reflections reflections = new Reflections(serviceRoot);
            Set<Class<?>> apiInterfaces = reflections.getTypesAnnotatedWith(Api.class);
            for(Class apiInterface : apiInterfaces){
                Set<Class<?>> apiImpls = reflections.getSubTypesOf(apiInterface);
                for(Class<?> apiImpl : apiImpls){
                    if(AnnotationUtils.findAnnotation(apiImpl, ApiServer.class) != null){
                        wrappedServices.put(apiInterface, apiImpl.newInstance());
                        ZKUtils.registerService(apiInterface.getName());
                        break;
                    }
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    void bind() throws UnknownHostException {
        this.context = ZMQ.context(1);
        this.socket = context.socket(ZMQ.REP);
        this.socket.bind("tcp://" + ZKUtils.getServiceBindAddress());
        new Thread(this).start();

    }
    private Object invoke(Command command) {
        if(command.getMethodArgs() != null && command.getMethodArgs().length > 0) {
            try {
                Class[] methodArgClasses = new Class[command.getMethodArgs().length];
                for(int i = 0; i < command.getMethodArgs().length; i++){
                    methodArgClasses[i] =  Class.forName(command.getMethodArgs()[i]);
                }
                Object wrappedService = wrappedServices.get(Class.forName(command.getServiceClass().replace("Transport", "")));
                Method m = wrappedService.getClass().getMethod(command.getMethodName(), methodArgClasses);
                Object result = m.invoke(wrappedService, command.getArgs());
                if(m.getReturnType().equals(Void.TYPE)){
                    return Void.TYPE;
                }else
                    return result;
            }catch (Exception e){
                return e.getCause();
            }
        } else {
            try {
                Object wrappedService = wrappedServices.get(Class.forName(command.getServiceClass().replace("Transport", "")));
                Method m = wrappedService.getClass().getMethod(command.getMethodName());
                Object result = m.invoke(wrappedService);
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