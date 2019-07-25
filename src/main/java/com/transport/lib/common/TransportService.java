package com.transport.lib.common;

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

/*
    Class responsible for initialization of transport subsystem
 */
public class TransportService {

    private final static Logger logger = LoggerFactory.getLogger(TransportService.class);

    // Known producer and consumer properties initialized in static context
    static final Properties producerProps = new Properties();
    static final Properties consumerProps = new Properties();

    // ZooKeeper client for checking topic existence and broker count
    static KafkaZkClient zkClient;
    // Current number of brokers in ZooKeeper cluster
    static int brokersCount = 0;
    // Mapping from primitives to associated wrappers
    static Map<Class<?>, Class<?>> primitiveToWrappers = new HashMap<>();
    // Topic names for server async topics: <class name>-<module.id>-server-async
    static Set<String> serverAsyncTopics;
    // Topic names for client async topics: <class name>-<module.id>-client-async
    static Set<String> clientAsyncTopics;
    // Topic names for server sync topics: <class name>-<module.id>-server-sync
    static Set<String> serverSyncTopics;

    // Initialized API implementations stored in a map, key - target service class, object - service instance
    private static Map<Class, Object> wrappedServices = new HashMap<>();
    // ZooKeeper client for topic creation
    private static AdminZkClient adminZkClient;
    // Topic names for client sync topics: <class name>-<module.id>-client-sync
    private static Set<String> clientSyncTopics;

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

    // User-provided set of API implementations as array of Classes
    @Autowired
    private ServerEndpoints serverEndpoints;

    // User-provided set of API client interfaces
    @Autowired
    private ClientEndpoints clientEndpoints;

    private List<KafkaReceiver> kafkaReceivers = new ArrayList<>();
    private List<Closeable> zmqReceivers = new ArrayList<>();
    private List<Thread> receiverThreads = new ArrayList<>();

    /*
        Get required JVM option or throw IllegalArgumentException
     */
    static String getRequiredOption(String option) {
        String optionValue = System.getProperty(option);
        if (optionValue == null || optionValue.trim().isEmpty())
            throw new IllegalArgumentException("Property " + option + "  was not set");
        else return optionValue;
    }

    /*
        Wait for Kafka cluster to rebalance itself
     */
    private static void waitForRebalance() {
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

    /*
        Get name of target server API implementation from class name of transport proxy received from client
     */
    private static Object getTargetService(Command command) throws ClassNotFoundException {
        return wrappedServices.get(Class.forName(command.getServiceClass().replace("Transport", "")));
    }

    /*
        Get target Method object basing on information available in Command received from client
     */
    static Method getTargetMethod(Command command) throws ClassNotFoundException, NoSuchMethodException {
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

    /*
        Invoke Command on some initialized API implementation instance
     */
    static Object invoke(Command command) {
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

    /*
        Get result from raw object after method execution
        Throwable objects are wrapped in ExceptionHolder instance because they are not serializable by Kryo
     */
    static Object getResult(Object result) {
        if (result instanceof Throwable) {
            StringWriter sw = new StringWriter();
            ((Throwable) result).printStackTrace(new PrintWriter(sw));
            return new ExceptionHolder(sw.toString());
        } else return result;
    }

    /*
        Register/publish server API implementations in ZooKeeper cluster
     */
    private void registerServices() throws Exception {
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

    /*
        Connect to ZooKeeper cluster, initialize ZooKeeper clients, create necessary topics in Kafka
     */
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

    /*
        Create necessary topics in Kafka cluster
     */
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
        apiImpls.forEach(x -> topicsCreated.add(x.getName() + "-" + getRequiredOption("module.id") + "-" + type));
        topicsCreated.forEach(topic -> {
            if (!zkClient.topicExists(topic))
                adminZkClient.createTopic(topic, brokersCount, 1, topicConfig, RackAwareMode.Disabled$.MODULE$);
            else if (!Integer.valueOf(zkClient.getTopicPartitionCount(topic).get() + "").equals(brokersCount))
                throw new IllegalStateException("Topic " + topic + " has wrong config");
        });
        return topicsCreated;
    }

    /*
        Transport subsystem initialization starts here
     */
    @PostConstruct
    private void init() throws Exception {
        try {
            // Measure startup time
            long startedTime = System.currentTimeMillis();
            // Connect to ZooKeeper cluster and create necessary Kafka topics using provided endpoints
            prepareServiceRegistration();
            // Multiple latches to control readiness of various receivers
            CountDownLatch started = null;
            // How many threads we need to start at the beginning?
            int expectedThreadCount = 0;
            // If we use Kafka
            if (Utils.useKafka()) {
                // One thread-consumer for receiving async responses from server
                // One consumer (not thread) for receiving sync responses from server
                if (!clientSyncTopics.isEmpty() && !clientAsyncTopics.isEmpty()) expectedThreadCount += 2;
                // One thread-consumer for receiving async requests from client
                // One thread-consumer for receiving sync requests from client
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

    /*
        Shutting down transport subsystem
     */
    public void close() {
        logger.info("Close started");

        try {
            for (String service : Utils.services) {
                Utils.delete(service, Protocol.ZMQ);
                Utils.delete(service, Protocol.KAFKA);
            }
            Utils.conn.close();
        } catch (Exception e) {
            logger.error("Unable to unregister services from ZooKeeper cluster", e);
        }

        this.kafkaReceivers.forEach(KafkaReceiver::close);

        this.zmqReceivers.forEach(a -> {
            try {
                a.close();
            } catch (Exception e) {
                logger.error("Unable to shut down ZeroMQ receivers", e);
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