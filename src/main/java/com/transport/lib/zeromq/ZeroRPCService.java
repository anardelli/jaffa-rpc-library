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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("WeakerAccess, unchecked")
public class ZeroRPCService implements Runnable {
    public static HashMap<Class, Object> wrappedServices = new HashMap<>();
    private static Context context;
    private static Socket socket;
    public static Map<Class<?>, Class<?>> map = new HashMap<>();
    public static volatile boolean active = false;
    static {
        map.put(boolean.class, Boolean.class);
        map.put(byte.class, Byte.class);
        map.put(short.class, Short.class);
        map.put(char.class, Character.class);
        map.put(int.class, Integer.class);
        map.put(long.class, Long.class);
        map.put(float.class, Float.class);
        map.put(double.class, Double.class);
        map.put(void.class, Void.class);
    }

    static String getOption(String option){
        String optionValue = System.getProperty(option);
        if(optionValue == null || optionValue.trim().isEmpty()) throw new IllegalArgumentException("Property service.root was not set");
        else return optionValue;
    }

    ZeroRPCService() {
        ZKUtils.connect(getOption("zookeeper.connection"));
        try{
            Reflections reflections = new Reflections(getOption("service.root"));
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
            context = ZMQ.context(1);
            socket = context.socket(ZMQ.REP);
            socket.bind("tcp://" + ZKUtils.getZeroMQBindAddress());
            active = true;
            new Thread(this).start();
            if(ZKUtils.useKafkaForAsync()) {
                new Thread( new KafkaRequestReceiver()).start();
                new Thread(new KafkaResponseReceiver()).start();
            }else
                new Thread( new CallbackReceiver()).start();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Object getTargetService(Command command) throws ClassNotFoundException{
        return wrappedServices.get(Class.forName(command.getServiceClass().replace("Transport", "")));
    }

    public static Method getTargetMethod(Command command) throws ClassNotFoundException, NoSuchMethodException {
        Object wrappedService = getTargetService(command);
        if(command.getMethodArgs() != null && command.getMethodArgs().length > 0) {
            Class[] methodArgClasses = new Class[command.getMethodArgs().length];
            for (int i = 0; i < command.getMethodArgs().length; i++) {
                methodArgClasses[i] = Class.forName(command.getMethodArgs()[i]);
            }
            return wrappedService.getClass().getMethod(command.getMethodName(), methodArgClasses);
        } else {
            return wrappedService.getClass().getMethod(command.getMethodName());
        }
    }

    public static Object invoke(Command command) {
        try {
            Object targetService = getTargetService(command);
            Method targetMethod = getTargetMethod(command);
            Object result;
            if(command.getMethodArgs() != null && command.getMethodArgs().length > 0)
                result = targetMethod.invoke(targetService, command.getArgs());
            else
                result = targetMethod.invoke(targetService);
            if(targetMethod.getReturnType().equals(Void.TYPE)) return Void.TYPE;
            else return result;
        }catch (Exception e){
            return e.getCause();
        }
    }

    public static Object getResult(Object result){
        if(result instanceof Throwable){
            StringWriter sw = new StringWriter();
            ((Throwable)result).printStackTrace(new PrintWriter(sw));
            return new ExceptionHolder(sw.toString());
        }else return result;
    }

    public void run() {
        while (active) {
            try {
                byte[] bytes = socket.recv();
                Kryo kryo = new Kryo();
                Input input = new Input(new ByteArrayInputStream(bytes));
                final Command command = kryo.readObject(input, Command.class);
                Object result = invoke(command);
                ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                Output output = new Output(bOutput);
                if(command.getCallbackKey() != null && command.getCallbackClass() != null){
                    socket.send("OK");
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
                    ZKUtils.closeSocketAndContext(socketAsync, contextAsync);
                }else{
                    kryo.writeClassAndObject(output, getResult(result));
                    output.close();
                    socket.send(bOutput.toByteArray());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Stopping services...");
        ZKUtils.closeSocketAndContext(socket, context);
    }
}