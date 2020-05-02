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
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.kafka.KafkaRequestSender;
import com.transport.lib.kafka.receivers.KafkaAsyncRequestReceiver;
import com.transport.lib.kafka.receivers.KafkaAsyncResponseReceiver;
import com.transport.lib.kafka.receivers.KafkaReceiver;
import com.transport.lib.kafka.receivers.KafkaSyncRequestReceiver;
import com.transport.lib.http.receivers.*;
import com.transport.lib.spring.ClientEndpoints;
import com.transport.lib.spring.ServerEndpoints;
import com.transport.lib.zeromq.receivers.ZMQAsyncAndSyncRequestReceiver;
import com.transport.lib.zeromq.receivers.ZMQAsyncResponseReceiver;
import com.transport.lib.zookeeper.Utils;
import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.KeeperException;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/*
    Class responsible for initialization of transport subsystem
 */
public class TransportService {

    // Known producer and consumer properties initialized in static context
    @Getter
    private static final Properties producerProps = new Properties();
    @Getter
    private static final Properties consumerProps = new Properties();
    // Mapping from primitives to associated wrappers
    private static final Map<Class<?>, Class<?>> primitiveToWrappers = new HashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(TransportService.class);
    // ZooKeeper client for checking topic existence and broker count
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static KafkaZkClient zkClient;
    // Current number of brokers in ZooKeeper cluster
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static int brokersCount = 0;
    // Topic names for server async topics: <class name>-<module.id>-server-async
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static Set<String> serverAsyncTopics;
    // Topic names for client async topics: <class name>-<module.id>-client-async
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static Set<String> clientAsyncTopics;
    // Topic names for server sync topics: <class name>-<module.id>-server-sync
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static Set<String> serverSyncTopics;
    // Topic names for client sync topics: <class name>-<module.id>-client-sync
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static Set<String> clientSyncTopics;
    // Initialized API implementations stored in a map, key - target service class, object - service instance
    private static final Map<Class<?>, Object> wrappedServices = new HashMap<>();
    // ZooKeeper client for topic creation
    @Setter(AccessLevel.PRIVATE)
    private static AdminZkClient adminZkClient;


    static {
        if(Utils.getTransportProtocol().equals(Protocol.KAFKA)){
            consumerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            consumerProps.put("enable.auto.commit", "false");
            consumerProps.put("group.id", UUID.randomUUID().toString());

            producerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        }
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

    private final List<KafkaReceiver> kafkaReceivers = new ArrayList<>();
    private final List<Closeable> zmqReceivers = new ArrayList<>();
    private final List<Thread> receiverThreads = new ArrayList<>();

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
        } catch (Exception e) {
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
        Responsible for constructing CallbackContainer
     */
    public static CallbackContainer constructCallbackContainer(Command command, Object result) throws ClassNotFoundException, NoSuchMethodException {
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
            Utils.registerService(apiImpl.getValue().getName(), Utils.getTransportProtocol());
        }
    }

    /*
        Connect to ZooKeeper cluster, initialize ZooKeeper clients, create necessary topics in Kafka
     */
    private void prepareServiceRegistration() throws ClassNotFoundException {
        Utils.connect(getRequiredOption("zookeeper.connection"));
        Protocol protocol = Utils.getTransportProtocol();
        if (protocol.equals(Protocol.KAFKA)) {
            ZooKeeperClient zooKeeperClient = new ZooKeeperClient(getRequiredOption("zookeeper.connection"), 200000, 15000, 10, Time.SYSTEM, UUID.randomUUID().toString(), UUID.randomUUID().toString());
            TransportService.setZkClient(new KafkaZkClient(zooKeeperClient, false, Time.SYSTEM));
            TransportService.setAdminZkClient(new AdminZkClient(zkClient));
            TransportService.setBrokersCount(zkClient.getAllBrokersInCluster().size());
            logger.info("Kafka brokers: {}", brokersCount);
            TransportService.setServerAsyncTopics(createTopics("server-async"));
            TransportService.setClientAsyncTopics(createTopics("client-async"));
            TransportService.setServerSyncTopics(createTopics("server-sync"));
            TransportService.setClientSyncTopics(createTopics("client-sync"));
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
                    server.getConstructor();
                } catch (NoSuchMethodException e) {
                    logger.error("General error during endpoint initialization", e);
                    throw new IllegalArgumentException(String.format("Class %s does not have default constructor!", server.getName()));
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
    @SuppressWarnings("unused")
    private void init() {
        try {
            // Measure startup time
            long startedTime = System.currentTimeMillis();
            // Connect to ZooKeeper cluster and create necessary Kafka topics using provided endpoints
            prepareServiceRegistration();
            // Multiple latches to control readiness of various receivers
            CountDownLatch started = null;
            // How many threads we need to start at the beginning?
            int expectedThreadCount = 0;

            Protocol protocol = Utils.getTransportProtocol();
            switch (protocol) {
                case KAFKA:
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
                        KafkaRequestSender.initSyncKafkaConsumers(brokersCount, started);
                        this.receiverThreads.add(new Thread(kafkaAsyncResponseReceiver));
                    }
                    break;
                case ZMQ:
                    // Construct ZeroMQ server receiver threads
                    if (serverEndpoints.getEndpoints().length != 0) {
                        ZMQAsyncAndSyncRequestReceiver zmqSyncRequestReceiver = new ZMQAsyncAndSyncRequestReceiver();
                        this.zmqReceivers.add(zmqSyncRequestReceiver);
                        this.receiverThreads.add(new Thread(zmqSyncRequestReceiver));
                    }
                    // Construct ZeroMQ client response receiver threads
                    if (clientEndpoints.getEndpoints().length != 0) {
                        ZMQAsyncResponseReceiver zmqAsyncResponseReceiver = new ZMQAsyncResponseReceiver();
                        this.zmqReceivers.add(zmqAsyncResponseReceiver);
                        this.receiverThreads.add(new Thread(zmqAsyncResponseReceiver));
                    }
                    break;
                case HTTP:
                    // Construct HTTP server receiver threads
                    if (serverEndpoints.getEndpoints().length != 0) {
                        HttpAsyncAndSyncRequestReceiver httpAsyncAndSyncRequestReceiver = new HttpAsyncAndSyncRequestReceiver();
                        this.zmqReceivers.add(httpAsyncAndSyncRequestReceiver);
                        this.receiverThreads.add(new Thread(httpAsyncAndSyncRequestReceiver));
                    }
                    // Construct HTTP client response receiver threads
                    if (clientEndpoints.getEndpoints().length != 0) {
                        HttpAsyncResponseReceiver httpAsyncResponseReceiver = new HttpAsyncResponseReceiver();
                        this.zmqReceivers.add(httpAsyncResponseReceiver);
                        this.receiverThreads.add(new Thread(httpAsyncResponseReceiver));
                    }
                    break;
                default:
                    throw new TransportSystemException("No known protocol defined");
            }
            // Start all threads
            this.receiverThreads.forEach(Thread::start);
            // And wait for CountDownLatch
            if (expectedThreadCount != 0) started.await();
            // Publish services in ZeroMQ
            registerServices();
            // Wait for Kafka cluster to be rebalanced
            if(protocol.equals(Protocol.KAFKA)) {
                RebalanceListener.waitForRebalance();
                logger.info("Initial balancing took: {}", RebalanceListener.lastRebalance - RebalanceListener.firstRebalance);
            }
            // Start finalizer
            FinalizationWorker.startFinalizer();
            logger.info("STARTED IN: {} ms", System.currentTimeMillis() - startedTime);
        } catch (Exception e) {
            logger.error("Exception during transport library startup:", e);
            throw new TransportSystemException(e);
        }
    }

    /*
        Shutting down transport subsystem
     */
    public void close() {
        logger.info("Close started");

        // Shut down Kafka consumers and associated threads
        this.kafkaReceivers.forEach(KafkaReceiver::close);
        logger.info("Kafka receivers closed");
        KafkaRequestSender.shutDownConsumers();
        logger.info("Kafka sync response consumers closed");
        // Unregister all server endpoints first
        try {
            for (String service : Utils.services) {
                Utils.delete(service, Protocol.ZMQ);
                Utils.delete(service, Protocol.KAFKA);
            }
            Utils.conn.close();
        } catch (KeeperException | InterruptedException | ParseException | UnknownHostException e) {
            logger.error("Unable to unregister services from ZooKeeper cluster", e);
            throw new TransportSystemException(e);
        }
        logger.info("Services were unregistered");
        // Shut down ZeroMQ receivers and associated threads
        this.zmqReceivers.forEach(a -> {
            try {
                a.close();
            } catch (IOException e) {
                logger.error("Unable to shut down ZeroMQ receivers", e);
                throw new TransportSystemException(e);
            }
        });
        logger.info("All ZMQ sockets were closed");
        // Kill all threads
        for (Thread thread : this.receiverThreads) {
            do {
                thread.interrupt();
            } while (thread.getState() != Thread.State.TERMINATED);
        }
        logger.info("All receiver threads stopped");
        // Stop finalizer threads
        FinalizationWorker.stopFinalizer();
        logger.info("Finalizer was stopped");
        logger.info("Transport shutdown completed");
    }
}