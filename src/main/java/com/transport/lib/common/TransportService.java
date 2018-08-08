package com.transport.lib.common;

import com.transport.lib.zookeeper.Utils;
import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.utils.Time;
import org.reflections.Reflections;
import org.springframework.core.annotation.AnnotationUtils;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.*;

@SuppressWarnings("WeakerAccess, unchecked")
public class TransportService {

    public static HashMap<Class, Object> wrappedServices = new HashMap<>();
    public static Context context;
    public static Socket socket;
    public static KafkaZkClient zkClient;
    public static int brokersCount = 0;
    public static AdminZkClient adminZkClient;
    public static Map<Class<?>, Class<?>> map = new HashMap<>();
    public static final Properties producerProps = new Properties();
    public static final Properties consumerProps = new Properties();

    static {
        consumerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("group.id", UUID.randomUUID().toString());

        producerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

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

    public static String getRequiredOption(String option){
        String optionValue = System.getProperty(option);
        if(optionValue == null || optionValue.trim().isEmpty()) throw new IllegalArgumentException("Property " + option + "  was not set");
        else return optionValue;
    }

    public static void registerServices() throws Exception{
        Reflections reflections = new Reflections(getRequiredOption("service.root"));
        Set<Class<?>> apiInterfaces = reflections.getTypesAnnotatedWith(Api.class);
        for(Class apiInterface : apiInterfaces){
            Set<Class<?>> apiImpls = reflections.getSubTypesOf(apiInterface);
            for(Class<?> apiImpl : apiImpls){
                if(AnnotationUtils.findAnnotation(apiImpl, ApiServer.class) != null){
                    wrappedServices.put(apiInterface, apiImpl.newInstance());
                    Utils.registerService(apiInterface.getName());
                    break;
                }
            }
        }
    }

    private static void prepareServiceRegistration() throws Exception{
        Utils.connect(getRequiredOption("zookeeper.connection"));
        zkClient = KafkaZkClient.apply(getRequiredOption("zookeeper.connection"),false,200000, 15000,10,Time.SYSTEM,UUID.randomUUID().toString(),UUID.randomUUID().toString());
        adminZkClient = new AdminZkClient(zkClient);
        context = ZMQ.context(1);
        socket = context.socket(ZMQ.REP);
        socket.bind("tcp://" + Utils.getZeroMQBindAddress());
        brokersCount = zkClient.getAllBrokersInCluster().size();
        HashSet<String> serverTopics = new HashSet<>();
        new Reflections(getRequiredOption("service.root")).getTypesAnnotatedWith(Api.class).forEach(x -> {if(x.isInterface()) serverTopics.add(x.getName() + "-" + getRequiredOption("module.id") + "-client-sync");});
        Properties topicConfig = new Properties();
        serverTopics.forEach(topic -> {
            if(!zkClient.topicExists(topic)) adminZkClient.createTopic(topic,brokersCount,1,topicConfig,RackAwareMode.Disabled$.MODULE$);
        });
    }

    TransportService() {
        try{
            prepareServiceRegistration();
            registerServices();
            new Thread( new ZMQSyncRequestReceiver()).start();
            new Thread( new KafkaSyncRequestReceiver()).start();
            new Thread( new KafkaAsyncRequestReceiver()).start();
            new Thread( new KafkaAsyncResponseReceiver()).start();
            new Thread( new ZMQAsyncResponseReceiver()).start();
        }catch (Exception e){
            System.out.println("Exception during transport library startup:");
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


}