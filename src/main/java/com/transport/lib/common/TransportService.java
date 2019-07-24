package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.transport.lib.zookeeper.Utils;
import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("all")
public class TransportService {

    private static Logger logger = LoggerFactory.getLogger(TransportService.class);

    public static final Properties producerProps = new Properties();
    public static final Properties consumerProps = new Properties();
    public static HashMap<Class, Object> wrappedServices = new HashMap<>();
    public static KafkaZkClient zkClient;
    public static int brokersCount = 0;
    public static AdminZkClient adminZkClient;
    public static Map<Class<?>, Class<?>> primitiveToWrappers = new HashMap<>();
    public static HashSet<String> serverAsyncTopics;
    public static HashSet<String> clientAsyncTopics;
    public static HashSet<String> serverSyncTopics;
    public static HashSet<String> clientSyncTopics;
    public static Kryo kryo = new Kryo();

    static {
        consumerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("group.id", UUID.randomUUID().toString());

        producerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        primitiveToWrappers.put(boolean.class, Boolean.class);
        primitiveToWrappers.put(byte.class, Byte.class);
        primitiveToWrappers.put(short.class, Short.class);
        primitiveToWrappers.put(char.class, Character.class);
        primitiveToWrappers.put(int.class, Integer.class);
        primitiveToWrappers.put(long.class, Long.class);
        primitiveToWrappers.put(float.class, Float.class);
        primitiveToWrappers.put(double.class, Double.class);
        primitiveToWrappers.put(void.class, Void.class);
    }

    @Autowired
    private ServerEndpoints serverEndpoints;
    @Autowired
    private ClientEndpoints clientEndpoints;
    private List<KafkaReceiver> kafkaReceivers = new ArrayList<>();
    private List<Closeable> zmqReceivers = new ArrayList<>();
    private List<Thread> receiverThreads = new ArrayList<>();

    public static String getRequiredOption(String option) {
        String optionValue = System.getProperty(option);
        if (optionValue == null || optionValue.trim().isEmpty())
            throw new IllegalArgumentException("Property " + option + "  was not set");
        else return optionValue;
    }

    public static void waitForRebalance() {
        long start = 0L;
        long lastRebalance = RebalanceListener.lastRebalance;
        while (true) {
            if (RebalanceListener.lastRebalance == lastRebalance) {
                if (start == 0L) {
                    start = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - start > 500) break;
            } else {
                start = 0L;
                lastRebalance = RebalanceListener.lastRebalance;
            }
        }
    }

    public static Object getTargetService(Command command) throws ClassNotFoundException {
        return wrappedServices.get(Class.forName(command.getServiceClass().replace("Transport", "")));
    }

    public static Method getTargetMethod(Command command) throws ClassNotFoundException, NoSuchMethodException {
        Object wrappedService = getTargetService(command);
        if (command.getMethodArgs() != null && command.getMethodArgs().length > 0) {
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
            if (command.getMethodArgs() != null && command.getMethodArgs().length > 0)
                result = targetMethod.invoke(targetService, command.getArgs());
            else
                result = targetMethod.invoke(targetService);
            if (targetMethod.getReturnType().equals(Void.TYPE)) return Void.TYPE;
            else return result;
        } catch (Exception e) {
            return e.getCause();
        }
    }

    public static Object getResult(Object result) {
        if (result instanceof Throwable) {
            StringWriter sw = new StringWriter();
            ((Throwable) result).printStackTrace(new PrintWriter(sw));
            return new ExceptionHolder(sw.toString());
        } else return result;
    }

    public void registerServices() throws Exception {
        Map<Class<?>, Class<?>> apiImpls = new HashMap<>();
        for (Class server : serverEndpoints.getServerEndpoints()) {
            logger.info("Server endpoint: " + server.getName());
            if (!server.isAnnotationPresent(ApiServer.class))
                throw new IllegalArgumentException("Class " + server.getName() + " is not annotated as ApiServer!");
            if (server.getInterfaces().length == 0)
                throw new IllegalArgumentException("Class " + server.getName() + " does not extend Api interface!");
            Class serverInterface = server.getInterfaces()[0];
            if (!serverInterface.isAnnotationPresent(Api.class))
                throw new IllegalArgumentException("Class " + server.getName() + " does not extend Api interface!");
            apiImpls.put(server, serverInterface);
        }
        for (Map.Entry<Class<?>, Class<?>> apiImpl : apiImpls.entrySet()) {
            wrappedServices.put(apiImpl.getValue(), apiImpl.getKey().newInstance());
            Utils.registerService(apiImpl.getValue().getName(), Utils.useKafka() ? Protocol.KAFKA : Protocol.ZMQ);
        }
    }

    private void prepareServiceRegistration() throws Exception {
        Utils.connect(getRequiredOption("zookeeper.connection"));
        if (Utils.useKafka()) {
            zkClient = KafkaZkClient.apply(getRequiredOption("zookeeper.connection"), false, 200000, 15000, 10, Time.SYSTEM, UUID.randomUUID().toString(), UUID.randomUUID().toString());
            adminZkClient = new AdminZkClient(zkClient);
            brokersCount = zkClient.getAllBrokersInCluster().size();
            logger.info("BROKER COUNT: " + brokersCount);
            serverAsyncTopics = createTopics("server-async");
            clientAsyncTopics = createTopics("client-async");
            serverSyncTopics = createTopics("server-sync");
            clientSyncTopics = createTopics("client-sync");
        }
    }

    private HashSet<String> createTopics(String type) throws Exception {
        Properties topicConfig = new Properties();
        HashSet<String> topicsCreated = new HashSet<>();
        Set<Class<?>> apiImpls = new HashSet<>();
        if (type.contains("server")) {
            for (Class server : serverEndpoints.getServerEndpoints()) {
                if (server.getInterfaces().length == 0)
                    throw new IllegalArgumentException("Class " + server.getName() + " does not extend Api interface!");
                Class serverInterface = server.getInterfaces()[0];
                if (!serverInterface.isAnnotationPresent(Api.class))
                    throw new IllegalArgumentException("Class " + server.getName() + " does not extend Api interface!");
                apiImpls.add(serverInterface);
            }
        } else {
            for (Class client : clientEndpoints.getClientEndpoints()) {
                if (!client.isAnnotationPresent(ApiClient.class))
                    throw new IllegalArgumentException("Class " + client.getName() + " does has ApiClient annotation!");
                apiImpls.add(Class.forName(client.getName().replace("Transport", "")));
            }
        }
        apiImpls.forEach(x -> {
            topicsCreated.add(x.getName() + "-" + getRequiredOption("module.id") + "-" + type);
        });
        topicsCreated.forEach(topic -> {
            if (!zkClient.topicExists(topic))
                adminZkClient.createTopic(topic, brokersCount, 1, topicConfig, RackAwareMode.Disabled$.MODULE$);
            else if (!Integer.valueOf(zkClient.getTopicPartitionCount(topic).get() + "").equals(brokersCount))
                throw new IllegalStateException("Topic " + topic + " has wrong config");
        });
        return topicsCreated;
    }

    @PostConstruct
    private void init() throws Exception {
        try {
            long startedTime = System.currentTimeMillis();
            prepareServiceRegistration();
            CountDownLatch started = null;
            int expectedThreadCount = 0;
            if (Utils.useKafka()) {
                if (!clientSyncTopics.isEmpty() && !clientAsyncTopics.isEmpty()) expectedThreadCount += 2;
                if (!serverSyncTopics.isEmpty() && !serverAsyncTopics.isEmpty()) expectedThreadCount += 2;
                if (expectedThreadCount != 0) started = new CountDownLatch(brokersCount * expectedThreadCount);
                if (!serverSyncTopics.isEmpty() && !serverAsyncTopics.isEmpty()) {
                    KafkaSyncRequestReceiver kafkaSyncRequestReceiver = new KafkaSyncRequestReceiver(started);
                    KafkaAsyncRequestReceiver kafkaAsyncRequestReceiver = new KafkaAsyncRequestReceiver(started);
                    this.kafkaReceivers.add(kafkaAsyncRequestReceiver);
                    this.kafkaReceivers.add(kafkaSyncRequestReceiver);
                    this.receiverThreads.add(new Thread(kafkaSyncRequestReceiver));
                    this.receiverThreads.add(new Thread(kafkaAsyncRequestReceiver));
                }
                if (!clientSyncTopics.isEmpty() && !clientAsyncTopics.isEmpty()) {
                    KafkaAsyncResponseReceiver kafkaAsyncResponseReceiver = new KafkaAsyncResponseReceiver(started);
                    this.kafkaReceivers.add(kafkaAsyncResponseReceiver);
                    Request.initSyncKafkaConsumers(brokersCount, started);
                    this.receiverThreads.add(new Thread(kafkaAsyncResponseReceiver));
                }
            } else {
                if (serverEndpoints.getServerEndpoints().length != 0) {
                    ZMQAsyncAndSyncRequestReceiver zmqSyncRequestReceiver = new ZMQAsyncAndSyncRequestReceiver();
                    this.zmqReceivers.add(zmqSyncRequestReceiver);
                    this.receiverThreads.add(new Thread(zmqSyncRequestReceiver));
                }
                if (clientEndpoints.getClientEndpoints().length != 0) {
                    ZMQAsyncResponseReceiver zmqAsyncResponseReceiver = new ZMQAsyncResponseReceiver();
                    this.zmqReceivers.add(zmqAsyncResponseReceiver);
                    this.receiverThreads.add(new Thread(zmqAsyncResponseReceiver));
                }
            }
            this.receiverThreads.forEach(Thread::start);
            if (expectedThreadCount != 0) started.await();
            registerServices();
            waitForRebalance();
            FinalizationWorker.startFinalizer();
            logger.info("STARTED IN: " + (System.currentTimeMillis() - startedTime) + " ms");
            logger.info("Initial rebalance took:" + (RebalanceListener.lastRebalance - RebalanceListener.firstRebalance));
        } catch (Exception e) {
            logger.error("Exception during transport library startup:", e);
            throw e;
        }
    }

    public void close() {
        logger.info("Close started");

        try {
            for (String service : Utils.services) {
                Utils.delete(service, Protocol.ZMQ);
                Utils.delete(service, Protocol.KAFKA);
            }
            Utils.conn.close();
        } catch (Exception e) {
        }

        this.kafkaReceivers.forEach(KafkaReceiver::close);

        this.zmqReceivers.forEach(a -> {
            try {
                a.close();
            } catch (Exception e) {
            }
        });

        for (Thread thread : this.receiverThreads) {
            do {
                thread.interrupt();
            } while (thread.getState() != Thread.State.TERMINATED);
        }

        FinalizationWorker.stopFinalizer();

        logger.info("Close finished");
    }
}