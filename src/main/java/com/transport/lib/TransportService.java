package com.transport.lib;

import com.transport.lib.annotations.Api;
import com.transport.lib.annotations.ApiClient;
import com.transport.lib.annotations.ApiServer;
import com.transport.lib.common.FinalizationWorker;
import com.transport.lib.common.RebalanceListener;
import com.transport.lib.entities.CallbackContainer;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.ExceptionHolder;
import com.transport.lib.entities.Protocol;
import com.transport.lib.receivers.*;
import com.transport.lib.request.RequestImpl;
import com.transport.lib.spring.ClientEndpoints;
import com.transport.lib.spring.ServerEndpoints;
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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/*
    Class responsible for initialization of transport subsystem
 */
public class TransportService {

    private static final Logger logger = LoggerFactory.getLogger(TransportService.class);

    // Known producer and consumer properties initialized in static context
    public static final Properties producerProps = new Properties();
    public static final Properties consumerProps = new Properties();

    // ZooKeeper client for checking topic existence and broker count
    public static KafkaZkClient zkClient;
    // Current number of brokers in ZooKeeper cluster
    public static int brokersCount = 0;
    // Mapping from primitives to associated wrappers
    public static Map<Class<?>, Class<?>> primitiveToWrappers = new HashMap<>();
    // Topic names for server async topics: <class name>-<module.id>-server-async
    public static Set<String> serverAsyncTopics;
    // Topic names for client async topics: <class name>-<module.id>-client-async
    public static Set<String> clientAsyncTopics;
    // Topic names for server sync topics: <class name>-<module.id>-server-sync
    public static Set<String> serverSyncTopics;

    // Initialized API implementations stored in a map, key - target service class, object - service instance
    private static Map<Class<?>, Object> wrappedServices = new HashMap<>();
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
    public static String getRequiredOption(String option) {
        // Take required JVM option
        String optionValue = System.getProperty(option);
        // Oops, it was not set - throw exception
        if (optionValue == null || optionValue.trim().isEmpty())
            throw new IllegalArgumentException("Property " + option + "  was not set");
        else return optionValue;
    }

    /*
        Wait for Kafka cluster to rebalance itself
     */
    private static void waitForRebalance() {
        long start = 0L;
        // Take last rebalance event
        long lastRebalance = RebalanceListener.lastRebalance;
        // Wait...
        while (true) {
            // Last rebalance time not changed
            if (RebalanceListener.lastRebalance == lastRebalance) {
                // Start counting time since that event
                if (start == 0L) {
                    start = System.currentTimeMillis();
                    // Wait for 500 ms since last event
                } else if (System.currentTimeMillis() - start > 500) break;
            } else {
                // Oops, new rebalance event, start waiting again
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
    private static Method getTargetMethod(Command command) throws ClassNotFoundException, NoSuchMethodException {
        // Take target API implementation instance from map
        Object wrappedService = getTargetService(command);
        // Client passed method arguments
        if (command.getMethodArgs() != null && command.getMethodArgs().length > 0) {
            // Convert array of class names to array of Classes
            Class<?>[] methodArgClasses = new Class[command.getMethodArgs().length];
            for (int i = 0; i < command.getMethodArgs().length; i++) {
                methodArgClasses[i] = Class.forName(command.getMethodArgs()[i]);
            }
            // And search Method
            return wrappedService.getClass().getMethod(command.getMethodName(), methodArgClasses);
        } else {
            // Just search method by name
            return wrappedService.getClass().getMethod(command.getMethodName());
        }
    }

    /*
        Invoke Command on some initialized API implementation instance
     */
    public static Object invoke(Command command) {
        try {
            // Take target API implementation instance
            Object targetService = getTargetService(command);
            // Take target API method
            Method targetMethod = getTargetMethod(command);
            // Save result of method invocation here
            Object result;
            // If arguments were passed by client - invoke method with them
            if (command.getMethodArgs() != null && command.getMethodArgs().length > 0)
                result = targetMethod.invoke(targetService, command.getArgs());
            else
                // Or without
                result = targetMethod.invoke(targetService);
            // If target Method return type is Void - return class Void as a stub
            if (targetMethod.getReturnType().equals(Void.TYPE)) return Void.TYPE;
            else return result;
        } catch (Throwable e) {
            // Exception occurred during target method invocation, save it
            return e.getCause();
        }
    }

    /*
        Get result from raw object after method execution
        Throwable objects are wrapped in ExceptionHolder instance because they are not serializable by Kryo
     */
    public static Object getResult(Object result) {
        // Exception occurred during target API method invocation
        if (result instanceof Throwable) {
            // Get stacktrace and save it in ExceptionHolder to transfer to client
            StringWriter sw = new StringWriter();
            ((Throwable) result).printStackTrace(new PrintWriter(sw));
            return new ExceptionHolder(sw.toString());
        } else return result;
    }

    /*
        Register/publish server API implementations in ZooKeeper cluster
     */
    private void registerServices() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        // Construct map <API interface> <API implementation class>
        Map<Class<?>, Class<?>> apiImpls = new HashMap<>();
        // Take user provided list of server endpoints that must be started
        for (Class<?> server : serverEndpoints.getEndpoints()) {
            logger.info("Server endpoint: {}", server.getName());
            // Add to target map
            apiImpls.put(server, server.getInterfaces()[0]);
        }

        for (Map.Entry<Class<?>, Class<?>> apiImpl : apiImpls.entrySet()) {
            // Initialize endpoint and add to map
            wrappedServices.put(apiImpl.getValue(), apiImpl.getKey().getDeclaredConstructor().newInstance());
            // Then register every API implementation endpoint
            Utils.registerService(apiImpl.getValue().getName(), Utils.useKafka() ? Protocol.KAFKA : Protocol.ZMQ);
        }
    }

    /*
        Connect to ZooKeeper cluster, initialize ZooKeeper clients, create necessary topics in Kafka
     */
    private void prepareServiceRegistration() throws ClassNotFoundException {
        Utils.connect(getRequiredOption("zookeeper.connection"));
        if (Utils.useKafka()) {
            zkClient = KafkaZkClient.apply(getRequiredOption("zookeeper.connection"), false, 200000, 15000, 10, Time.SYSTEM, UUID.randomUUID().toString(), UUID.randomUUID().toString());
            adminZkClient = new AdminZkClient(zkClient);
            brokersCount = zkClient.getAllBrokersInCluster().size();
            logger.info("Kafka brokers: {}", brokersCount);
            serverAsyncTopics = createTopics("server-async");
            clientAsyncTopics = createTopics("client-async");
            serverSyncTopics = createTopics("server-sync");
            clientSyncTopics = createTopics("client-sync");
        }
    }

    /*
        Create necessary topics in Kafka cluster
        type - parameter that acts as both suffix of topic' name and type
     */
    private Set<String> createTopics(String type) throws ClassNotFoundException {
        // Topics that were created
        Set<String> topicsCreated = new HashSet<>();
        Set<Class<?>> apiImpls = new HashSet<>();
        // First, we need to construct list of classes - server and client endpoints that must be initialized
        if (type.contains("server")) {
            // Take all server endpoints and
            for (Class<?> server : serverEndpoints.getEndpoints()) {
                // If endpoint implementation is not annotated - it's an error
                if (!server.isAnnotationPresent(ApiServer.class))
                    throw new IllegalArgumentException(String.format("Class %s is not annotated as ApiServer!", server.getName()));
                // If endpoint implementation does not implement any interfaces - it's an error
                if (server.getInterfaces().length == 0)
                    throw new IllegalArgumentException(String.format("Class %s does not extend Api interface!", server.getName()));
                // If first implemented interface not annotated as @Api - it's an error
                Class<?> serverInterface = server.getInterfaces()[0];
                if (!serverInterface.isAnnotationPresent(Api.class))
                    throw new IllegalArgumentException(String.format("Class %s does not extend Api interface!", server.getName()));
                try {
                    // API implementation must have default constructor
                    if (server.getConstructor() == null)
                        throw new IllegalArgumentException(String.format("Class %s does not have default constructor!", server.getName()));
                }catch (NoSuchMethodException e){
                    logger.error("General error during endpoint initialization", e);
                }
                apiImpls.add(serverInterface);
            }
        } else {
            // For client endpoint
            for (Class<?> client : clientEndpoints.getEndpoints()) {
                // We only check @ApiClient annotation presence
                if (!client.isAnnotationPresent(ApiClient.class))
                    throw new IllegalArgumentException("Class " + client.getName() + " does has ApiClient annotation!");
                apiImpls.add(Class.forName(client.getName().replace("Transport", "")));
            }
        }
        // Construct topic names
        apiImpls.forEach(x -> topicsCreated.add(x.getName() + "-" + getRequiredOption("module.id") + "-" + type));
        // And create topics if not exist
        topicsCreated.forEach(topic -> {
            if (!zkClient.topicExists(topic))
                adminZkClient.createTopic(topic, brokersCount, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
            else if (!Integer.valueOf(zkClient.getTopicPartitionCount(topic).get() + "").equals(brokersCount))
                // If topic exists but has wrong number of partitions
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
                // Number of threads-consumer and consumers expected to start - expectedThreadCount * number of brokers
                if (expectedThreadCount != 0) started = new CountDownLatch(brokersCount * expectedThreadCount);
                // Construct server consumer threads
                if (!serverSyncTopics.isEmpty() && !serverAsyncTopics.isEmpty()) {
                    KafkaSyncRequestReceiver kafkaSyncRequestReceiver = new KafkaSyncRequestReceiver(started);
                    KafkaAsyncRequestReceiver kafkaAsyncRequestReceiver = new KafkaAsyncRequestReceiver(started);
                    this.kafkaReceivers.add(kafkaAsyncRequestReceiver);
                    this.kafkaReceivers.add(kafkaSyncRequestReceiver);
                    this.receiverThreads.add(new Thread(kafkaSyncRequestReceiver));
                    this.receiverThreads.add(new Thread(kafkaAsyncRequestReceiver));
                }
                // Construct client consumer threads and just consumers (for sync calls)
                if (!clientSyncTopics.isEmpty() && !clientAsyncTopics.isEmpty()) {
                    KafkaAsyncResponseReceiver kafkaAsyncResponseReceiver = new KafkaAsyncResponseReceiver(started);
                    this.kafkaReceivers.add(kafkaAsyncResponseReceiver);
                    RequestImpl.initSyncKafkaConsumers(brokersCount, started);
                    this.receiverThreads.add(new Thread(kafkaAsyncResponseReceiver));
                }
            } else {
                // Construct ZeroMQ server receiver threads
                if (serverEndpoints.getEndpoints().length != 0) {
                    ZMQAsyncAndSyncRequestReceiver zmqSyncRequestReceiver = new ZMQAsyncAndSyncRequestReceiver();
                    this.zmqReceivers.add(zmqSyncRequestReceiver);
                    this.receiverThreads.add(new Thread(zmqSyncRequestReceiver));
                }
                // Construct ZeroMQ client receiver threads
                if (clientEndpoints.getEndpoints().length != 0) {
                    ZMQAsyncResponseReceiver zmqAsyncResponseReceiver = new ZMQAsyncResponseReceiver();
                    this.zmqReceivers.add(zmqAsyncResponseReceiver);
                    this.receiverThreads.add(new Thread(zmqAsyncResponseReceiver));
                }
            }
            // Start all threads
            this.receiverThreads.forEach(Thread::start);
            // And wait for CountDownLatch
            if (expectedThreadCount != 0) started.await();
            // Publish services in ZeroMQ
            registerServices();
            // Wait for Kafka cluster rebalance
            waitForRebalance();
            // Start finalizer
            FinalizationWorker.startFinalizer();
            logger.info("STARTED IN: {} ms", System.currentTimeMillis() - startedTime);
            logger.info("Initial rebalance took: {}", RebalanceListener.lastRebalance - RebalanceListener.firstRebalance);
        } catch (Exception e) {
            logger.error("Exception during transport library startup:", e);
            throw e;
        }
    }

    /*
        Responsible for constructing CallbackContainer
     */
    public static CallbackContainer constructCallbackContainer(Command command, Object result) throws ClassNotFoundException, NoSuchMethodException{
        // Construct CallbackContainer
        CallbackContainer callbackContainer = new CallbackContainer();
        // User-provided callback key for identifying original request
        callbackContainer.setKey(command.getCallbackKey());
        // Fully-qualified Callback class name
        callbackContainer.setListener(command.getCallbackClass());
        // Result object or ExceptionHolder instance
        callbackContainer.setResult(getResult(result));
        // If target method returned primitive object, then send back wrapper as result class
        Method targetMethod = getTargetMethod(command);
        if (primitiveToWrappers.containsKey(targetMethod.getReturnType())) {
            callbackContainer.setResultClass(primitiveToWrappers.get(targetMethod.getReturnType()).getName());
        } else {
            callbackContainer.setResultClass(targetMethod.getReturnType().getName());
        }
        return callbackContainer;
    }

    /*
        Shutting down transport subsystem
     */
    public void close() {
        logger.info("Close started");

        // Unregister all server endpoints first
        try {
            for (String service : Utils.services) {
                Utils.delete(service, Protocol.ZMQ);
                Utils.delete(service, Protocol.KAFKA);
            }
            Utils.conn.close();
        } catch (Exception e) {
            logger.error("Unable to unregister services from ZooKeeper cluster", e);
        }

        // Shut down Kafka consumers and associated threads
        this.kafkaReceivers.forEach(KafkaReceiver::close);

        // Shut down ZeroMQ receivers and associated threads
        this.zmqReceivers.forEach(a -> {
            try {
                a.close();
            } catch (Exception e) {
                logger.error("Unable to shut down ZeroMQ receivers", e);
            }
        });

        // Kill all threads
        for (Thread thread : this.receiverThreads) {
            do {
                thread.interrupt();
            } while (thread.getState() != Thread.State.TERMINATED);
        }

        // Stop finalizer threads
        FinalizationWorker.stopFinalizer();

        logger.info("Transport subsystem shut down");
    }
}