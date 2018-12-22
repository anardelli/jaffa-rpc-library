package com.transport.lib.common;

import com.transport.lib.zookeeper.Utils;
import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.utils.Time;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotationUtils;

import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("all")
public class TransportService {

    private static Logger logger = LoggerFactory.getLogger(TransportService.class);

    public static HashMap<Class, Object> wrappedServices = new HashMap<>();
    public static KafkaZkClient zkClient;
    public static int brokersCount = 0;
    public static AdminZkClient adminZkClient;
    public static Map<Class<?>, Class<?>> map = new HashMap<>();
    public static final Properties producerProps = new Properties();
    public static final Properties consumerProps = new Properties();

    public static HashSet<String> serverAsyncTopics;
    public static HashSet<String> clientAsyncTopics;
    public static HashSet<String> serverSyncTopics;
    public static HashSet<String> clientSyncTopics;

    private List<KafkaReceiver> kafkaReceivers = new ArrayList<>();
    private List<Closeable> zmqReceivers = new ArrayList<>();
    private List<Thread> receiverThreads = new ArrayList<>();

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

    public void registerServices() throws Exception{
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

    private void prepareServiceRegistration() throws Exception{
        Utils.connect(getRequiredOption("zookeeper.connection"));
        zkClient = KafkaZkClient.apply(getRequiredOption("zookeeper.connection"),false,200000, 15000,10,Time.SYSTEM,UUID.randomUUID().toString(),UUID.randomUUID().toString());
        adminZkClient = new AdminZkClient(zkClient);
        brokersCount = zkClient.getAllBrokersInCluster().size();
        logger.info("BROKER COUNT: " + brokersCount);
        serverAsyncTopics = createTopics("server-async");
        clientAsyncTopics = createTopics("client-async");
        serverSyncTopics = createTopics( "server-sync");
        clientSyncTopics = createTopics( "client-sync");
    }

    private HashSet<String> createTopics(String type){
        Properties topicConfig = new Properties();
        HashSet<String> topicsCreated = new HashSet<>();
        new Reflections(getRequiredOption("service.root")).getTypesAnnotatedWith(Api.class).forEach(x -> {if(x.isInterface()) topicsCreated.add(x.getName() + "-" + getRequiredOption("module.id") + "-" + type);});
        topicsCreated.forEach(topic -> {
            if(!zkClient.topicExists(topic)) adminZkClient.createTopic(topic,brokersCount,1,topicConfig,RackAwareMode.Disabled$.MODULE$);
            else if(!Integer.valueOf(zkClient.getTopicPartitionCount(topic).get()+"").equals(brokersCount)) throw new IllegalStateException("Topic " + topic + " has wrong config");
        });
        return topicsCreated;
    }

    TransportService() {
        try{
            long startedTime = System.currentTimeMillis();
            prepareServiceRegistration();
            CountDownLatch started = new CountDownLatch(brokersCount * 4);

            ZMQAsyncAndSyncRequestReceiver zmqSyncRequestReceiver = new ZMQAsyncAndSyncRequestReceiver();
            ZMQAsyncResponseReceiver zmqAsyncResponseReceiver = new ZMQAsyncResponseReceiver();
            KafkaSyncRequestReceiver kafkaSyncRequestReceiver = new KafkaSyncRequestReceiver(started);
            KafkaAsyncRequestReceiver kafkaAsyncRequestReceiver = new KafkaAsyncRequestReceiver(started);
            KafkaAsyncResponseReceiver kafkaAsyncResponseReceiver = new KafkaAsyncResponseReceiver(started);

            Request.initSyncKafkaConsumers(brokersCount, started);

            this.kafkaReceivers.add(kafkaAsyncRequestReceiver);
            this.kafkaReceivers.add(kafkaAsyncResponseReceiver);
            this.kafkaReceivers.add(kafkaSyncRequestReceiver);

            this.zmqReceivers.add(zmqAsyncResponseReceiver);
            this.zmqReceivers.add(zmqSyncRequestReceiver);

            this.receiverThreads.add(new Thread(zmqSyncRequestReceiver));
            this.receiverThreads.add(new Thread(zmqAsyncResponseReceiver));
            this.receiverThreads.add(new Thread(kafkaSyncRequestReceiver));
            this.receiverThreads.add(new Thread(kafkaAsyncRequestReceiver));
            this.receiverThreads.add(new Thread(kafkaAsyncResponseReceiver));

            this.receiverThreads.forEach(Thread::start);

            started.await();
            registerServices();
            waitForRebalance();
            logger.info("STARTED IN: " + (System.currentTimeMillis() - startedTime) + " ms");
            logger.info("Initial rebalance took:" + (RebalanceListener.lastRebalance -  RebalanceListener.firstRebalance));
        }catch (Exception e){
            logger.error("Exception during transport library startup:", e);
        }
    }

    public static void waitForRebalance(){
        long start = 0L;
        long lastRebalance = RebalanceListener.lastRebalance;
        while(true) {
            if (RebalanceListener.lastRebalance == lastRebalance){
                if(start == 0L){
                    start = System.currentTimeMillis();
                }else if(System.currentTimeMillis() - start > 500) break;
            } else {
                start = 0L;
                lastRebalance =  RebalanceListener.lastRebalance;
            }
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

    public void close(){
        logger.info("Close started");

        this.kafkaReceivers.forEach(KafkaReceiver::close);

        this.zmqReceivers.forEach(a -> { try { a.close(); } catch(Exception e) { } });

        for(Thread thread: this.receiverThreads){
            do {
                thread.interrupt();
            }while(thread.getState() != Thread.State.TERMINATED);
        }

        try{
            for(String service : Utils.services){
                Utils.delete(service);
            }
            Utils.conn.close();
        }catch (Exception e){ }

        logger.info("Close finished");
    }
}