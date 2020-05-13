package com.jaffa.rpc.lib;

import com.jaffa.rpc.lib.annotations.ApiClient;
import com.jaffa.rpc.lib.entities.CallbackContainer;
import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.entities.ExceptionHolder;
import com.jaffa.rpc.lib.entities.Protocol;
import com.jaffa.rpc.lib.exception.JaffaRpcSystemException;
import com.jaffa.rpc.lib.kafka.KafkaRequestSender;
import com.jaffa.rpc.lib.zeromq.CurveUtils;
import com.jaffa.rpc.lib.zeromq.ZeroMqRequestSender;
import com.jaffa.rpc.lib.annotations.Api;
import com.jaffa.rpc.lib.annotations.ApiServer;
import com.jaffa.rpc.lib.common.FinalizationWorker;
import com.jaffa.rpc.lib.common.RebalancedListener;
import com.jaffa.rpc.lib.http.receivers.HttpAsyncAndSyncRequestReceiver;
import com.jaffa.rpc.lib.http.receivers.HttpAsyncResponseReceiver;
import com.jaffa.rpc.lib.kafka.receivers.KafkaAsyncRequestReceiver;
import com.jaffa.rpc.lib.kafka.receivers.KafkaAsyncResponseReceiver;
import com.jaffa.rpc.lib.kafka.receivers.KafkaReceiver;
import com.jaffa.rpc.lib.kafka.receivers.KafkaSyncRequestReceiver;
import com.jaffa.rpc.lib.rabbitmq.RabbitMQRequestSender;
import com.jaffa.rpc.lib.rabbitmq.receivers.RabbitMQAsyncAndSyncRequestReceiver;
import com.jaffa.rpc.lib.rabbitmq.receivers.RabbitMQAsyncResponseReceiver;
import com.jaffa.rpc.lib.spring.ClientEndpoints;
import com.jaffa.rpc.lib.spring.ServerEndpoints;
import com.jaffa.rpc.lib.zeromq.receivers.ZMQAsyncAndSyncRequestReceiver;
import com.jaffa.rpc.lib.zeromq.receivers.ZMQAsyncResponseReceiver;
import com.jaffa.rpc.lib.zookeeper.Utils;
import kafka.admin.RackAwareMode;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.utils.Time;
import org.apache.zookeeper.KeeperException;
import org.json.simple.parser.ParseException;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.beans.factory.annotation.Autowired;
import org.zeromq.ZContext;

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

@Slf4j
@SuppressWarnings("squid:S2142")
public class JaffaService {

    @Getter
    private static final Properties producerProps = new Properties();
    @Getter
    private static final Properties consumerProps = new Properties();
    private static final Map<Class<?>, Class<?>> primitiveToWrappers = new HashMap<>();
    private static final Map<Class<?>, Object> wrappedServices = new HashMap<>();
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static KafkaZkClient zkClient;
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static int brokersCount = 0;
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static Set<String> serverAsyncTopics;
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static Set<String> clientAsyncTopics;
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static Set<String> serverSyncTopics;
    @Getter
    @Setter(AccessLevel.PRIVATE)
    private static Set<String> clientSyncTopics;
    @Setter(AccessLevel.PRIVATE)
    private static AdminZkClient adminZkClient;
    @Setter(AccessLevel.PRIVATE)
    private static RabbitAdmin adminRabbitMQ;
    @Setter(AccessLevel.PRIVATE)
    @Getter(AccessLevel.PUBLIC)
    private static ConnectionFactory connectionFactory;

    private static void initInternalProps() {
        if (Utils.getRpcProtocol().equals(Protocol.KAFKA)) {
            consumerProps.put("bootstrap.servers", getRequiredOption("jaffa.rpc.protocol.kafka.bootstrap.servers"));
            consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
            consumerProps.put("enable.auto.commit", "false");
            consumerProps.put("group.id", UUID.randomUUID().toString());

            producerProps.put("bootstrap.servers", getRequiredOption("jaffa.rpc.protocol.kafka.bootstrap.servers"));
            producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

            if(Boolean.parseBoolean(System.getProperty("jaffa.rpc.protocol.kafka.use.ssl", "false"))){
                Map<String, String> sslProps = new HashMap<>();
                sslProps.put("security.protocol", "SSL");
                sslProps.put("ssl.truststore.location", System.getProperty("jaffa.rpc.protocol.kafka.ssl.truststore.location"));
                sslProps.put("ssl.truststore.password", System.getProperty("jaffa.rpc.protocol.kafka.ssl.truststore.password"));
                sslProps.put("ssl.keystore.location",   System.getProperty("jaffa.rpc.protocol.kafka.ssl.keystore.location"));
                sslProps.put("ssl.keystore.password",   System.getProperty("jaffa.rpc.protocol.kafka.ssl.keystore.password"));
                sslProps.put("ssl.key.password",        System.getProperty("jaffa.rpc.protocol.kafka.ssl.key.password"));
                consumerProps.putAll(sslProps);
                producerProps.putAll(sslProps);
            }
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

    private final List<KafkaReceiver> kafkaReceivers = new ArrayList<>();
    private final List<Closeable> zmqReceivers = new ArrayList<>();
    private final List<Thread> receiverThreads = new ArrayList<>();
    @Autowired
    private ServerEndpoints serverEndpoints;
    @Autowired
    private ClientEndpoints clientEndpoints;

    public static String getRequiredOption(String option) {
        String optionValue = System.getProperty(option);
        if (optionValue == null || optionValue.trim().isEmpty())
            throw new IllegalArgumentException("Property " + option + "  was not set");
        else return optionValue;
    }

    private static Object getTargetService(Command command) throws ClassNotFoundException {
        return wrappedServices.get(Class.forName(Utils.getServiceInterfaceNameFromClient(command.getServiceClass())));
    }

    private static Method getTargetMethod(Command command) throws ClassNotFoundException, NoSuchMethodException {
        Object wrappedService = getTargetService(command);
        if (command.getMethodArgs() != null && command.getMethodArgs().length > 0) {
            Class<?>[] methodArgClasses = new Class[command.getMethodArgs().length];
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

    public static CallbackContainer constructCallbackContainer(Command command, Object result) throws ClassNotFoundException, NoSuchMethodException {
        CallbackContainer callbackContainer = new CallbackContainer();
        callbackContainer.setKey(command.getCallbackKey());
        callbackContainer.setListener(command.getCallbackClass());
        callbackContainer.setResult(getResult(result));
        Method targetMethod = getTargetMethod(command);
        if (primitiveToWrappers.containsKey(targetMethod.getReturnType())) {
            callbackContainer.setResultClass(primitiveToWrappers.get(targetMethod.getReturnType()).getName());
        } else {
            callbackContainer.setResultClass(targetMethod.getReturnType().getName());
        }
        return callbackContainer;
    }

    private void registerServices() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Map<Class<?>, Class<?>> apiImpls = new HashMap<>();
        for (Class<?> server : serverEndpoints.getEndpoints()) {
            log.info("Server endpoint: {}", server.getName());
            apiImpls.put(server, server.getInterfaces()[0]);
        }
        for (Map.Entry<Class<?>, Class<?>> apiImpl : apiImpls.entrySet()) {
            wrappedServices.put(apiImpl.getValue(), apiImpl.getKey().getDeclaredConstructor().newInstance());
            Utils.registerService(apiImpl.getValue().getName(), Utils.getRpcProtocol());
        }
    }

    @SuppressWarnings("squid:S2583")
    private void prepareServiceRegistration() throws ClassNotFoundException {
        Utils.connect(getRequiredOption("zookeeper.connection"));
        Protocol protocol = Utils.getRpcProtocol();
        if (protocol.equals(Protocol.KAFKA)) {
            ZooKeeperClient zooKeeperClient = new ZooKeeperClient(getRequiredOption("zookeeper.connection"), 200000, 15000, 10, Time.SYSTEM, UUID.randomUUID().toString(), UUID.randomUUID().toString());
            JaffaService.setZkClient(new KafkaZkClient(zooKeeperClient, false, Time.SYSTEM));
            JaffaService.setAdminZkClient(new AdminZkClient(zkClient));
            JaffaService.setBrokersCount(zkClient.getAllBrokersInCluster().size());
            log.info("Kafka brokers: {}", brokersCount);
            JaffaService.setServerAsyncTopics(createKafkaTopics("server-async"));
            JaffaService.setClientAsyncTopics(createKafkaTopics("client-async"));
            JaffaService.setServerSyncTopics(createKafkaTopics("server-sync"));
            JaffaService.setClientSyncTopics(createKafkaTopics("client-sync"));
        }
        if (protocol.equals(Protocol.RABBIT)) {
            JaffaService.setConnectionFactory(new CachingConnectionFactory(getRequiredOption("jaffa.rpc.rabbit.host"), Integer.parseInt(getRequiredOption("jaffa.rpc.rabbit.port"))));
            JaffaService.setAdminRabbitMQ(new RabbitAdmin(JaffaService.connectionFactory));
            JaffaService.adminRabbitMQ.declareExchange(new DirectExchange(RabbitMQRequestSender.EXCHANGE_NAME, true, false));
            if (JaffaService.adminRabbitMQ.getQueueInfo(RabbitMQRequestSender.SERVER) == null) {
                JaffaService.adminRabbitMQ.declareQueue(new Queue(RabbitMQRequestSender.SERVER));
            }
            if (JaffaService.adminRabbitMQ.getQueueInfo(RabbitMQRequestSender.CLIENT_ASYNC_NAME) == null) {
                JaffaService.adminRabbitMQ.declareQueue(new Queue(RabbitMQRequestSender.CLIENT_ASYNC_NAME));
            }
            if (JaffaService.adminRabbitMQ.getQueueInfo(RabbitMQRequestSender.CLIENT_SYNC_NAME) == null) {
                JaffaService.adminRabbitMQ.declareQueue(new Queue(RabbitMQRequestSender.CLIENT_SYNC_NAME));
            }
        }
    }

    private Set<String> getTopicNames(String type) throws ClassNotFoundException {
        Set<String> topicsCreated = new HashSet<>();
        Set<Class<?>> apiImpls = new HashSet<>();
        if (type.contains("server")) {
            for (Class<?> server : serverEndpoints.getEndpoints()) {
                if (!server.isAnnotationPresent(ApiServer.class))
                    throw new IllegalArgumentException(String.format("Class %s is not annotated as ApiServer!", server.getName()));
                if (server.getInterfaces().length == 0)
                    throw new IllegalArgumentException(String.format("Class %s does not extend Api interface!", server.getName()));
                Class<?> serverInterface = server.getInterfaces()[0];
                if (!serverInterface.isAnnotationPresent(Api.class))
                    throw new IllegalArgumentException(String.format("Class %s does not extend Api interface!", server.getName()));
                try {
                    server.getConstructor();
                } catch (NoSuchMethodException e) {
                    log.error("General error during endpoint initialization", e);
                    throw new IllegalArgumentException(String.format("Class %s does not have default constructor!", server.getName()));
                }
                apiImpls.add(serverInterface);
            }
        } else {
            for (Class<?> client : clientEndpoints.getEndpoints()) {
                if (!client.isAnnotationPresent(ApiClient.class))
                    throw new IllegalArgumentException("Class " + client.getName() + " does has ApiClient annotation!");
                apiImpls.add(Class.forName(Utils.getServiceInterfaceNameFromClient(client.getName())));
            }
        }
        apiImpls.forEach(x -> topicsCreated.add(x.getName() + "-" + getRequiredOption("module.id") + "-" + type));
        return topicsCreated;
    }

    private Set<String> createKafkaTopics(String type) throws ClassNotFoundException {
        Set<String> topicsCreated = getTopicNames(type);
        topicsCreated.forEach(topic -> {
            if (!zkClient.topicExists(topic))
                adminZkClient.createTopic(topic, brokersCount, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
            else if (!Integer.valueOf(zkClient.getTopicPartitionCount(topic).get() + "").equals(brokersCount))
                throw new IllegalStateException("Topic " + topic + " has wrong config");
        });
        return topicsCreated;
    }

    @PostConstruct
    @SuppressWarnings("unused")
    private void init() {
        try {
            Utils.loadProperties();
            initInternalProps();
            long startedTime = System.currentTimeMillis();
            prepareServiceRegistration();
            CountDownLatch started = null;
            int expectedThreadCount = 0;
            Protocol protocol = Utils.getRpcProtocol();
            switch (protocol) {
                case KAFKA:
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
                        KafkaRequestSender.initSyncKafkaConsumers(brokersCount, started);
                        this.receiverThreads.add(new Thread(kafkaAsyncResponseReceiver));
                    }
                    break;
                case ZMQ:
                    if (serverEndpoints.getEndpoints().length != 0) {
                        ZMQAsyncAndSyncRequestReceiver zmqSyncRequestReceiver = new ZMQAsyncAndSyncRequestReceiver();
                        this.zmqReceivers.add(zmqSyncRequestReceiver);
                        this.receiverThreads.add(new Thread(zmqSyncRequestReceiver));
                    }
                    if (clientEndpoints.getEndpoints().length != 0) {
                        ZMQAsyncResponseReceiver zmqAsyncResponseReceiver = new ZMQAsyncResponseReceiver();
                        this.zmqReceivers.add(zmqAsyncResponseReceiver);
                        this.receiverThreads.add(new Thread(zmqAsyncResponseReceiver));
                    }
                    if (Boolean.parseBoolean(System.getProperty("jaffa.rpc.protocol.zmq.curve.enabled", "false"))) {
                        CurveUtils.readClientKeys();
                        CurveUtils.readServerKeys();
                    }
                    break;
                case HTTP:
                    if (serverEndpoints.getEndpoints().length != 0) {
                        HttpAsyncAndSyncRequestReceiver httpAsyncAndSyncRequestReceiver = new HttpAsyncAndSyncRequestReceiver();
                        this.zmqReceivers.add(httpAsyncAndSyncRequestReceiver);
                        this.receiverThreads.add(new Thread(httpAsyncAndSyncRequestReceiver));
                    }
                    if (clientEndpoints.getEndpoints().length != 0) {
                        HttpAsyncResponseReceiver httpAsyncResponseReceiver = new HttpAsyncResponseReceiver();
                        this.zmqReceivers.add(httpAsyncResponseReceiver);
                        this.receiverThreads.add(new Thread(httpAsyncResponseReceiver));
                    }
                    break;
                case RABBIT:
                    if (serverEndpoints.getEndpoints().length != 0) {
                        RabbitMQAsyncAndSyncRequestReceiver rabbitMQAsyncAndSyncRequestReceiver = new RabbitMQAsyncAndSyncRequestReceiver();
                        this.zmqReceivers.add(rabbitMQAsyncAndSyncRequestReceiver);
                        this.receiverThreads.add(new Thread(rabbitMQAsyncAndSyncRequestReceiver));
                    }
                    if (clientEndpoints.getEndpoints().length != 0) {
                        RabbitMQAsyncResponseReceiver rabbitMQAsyncResponseReceiver = new RabbitMQAsyncResponseReceiver();
                        this.zmqReceivers.add(rabbitMQAsyncResponseReceiver);
                        this.receiverThreads.add(new Thread(rabbitMQAsyncResponseReceiver));
                    }
                    RabbitMQRequestSender.init();
                    break;
                default:
                    throw new JaffaRpcSystemException("No known protocol defined");
            }
            this.receiverThreads.forEach(Thread::start);
            if (expectedThreadCount != 0) started.await();
            registerServices();
            if (protocol.equals(Protocol.KAFKA)) {
                RebalancedListener.waitForRebalanced();
                log.info("Initial balancing took: {}", RebalancedListener.lastEvent - RebalancedListener.firstEvent);
            }
            FinalizationWorker.startFinalizer();
            log.info("\n    .---.                                             \n" +
                    "    |   |                                               \n" +
                    "    '---'                                               \n" +
                    "    .---.                 _.._       _.._               \n" +
                    "    |   |               .' .._|    .' .._|              \n" +
                    "    |   |     __        | '        | '         __       \n" +
                    "    |   |  .:--.'.    __| |__    __| |__    .:--.'.     \n" +
                    "    |   | / |   \\ |  |__   __|  |__   __|  / |   \\ |  \n" +
                    "    |   | `\" __ | |     | |        | |     `\" __ | |  \n" +
                    "    |   |  .'.''| |     | |        | |      .'.''| |    \n" +
                    " __.'   ' / /   | |_    | |        | |     / /   | |_   \n" +
                    "|      '  \\ \\._,\\ '/    | |        | |     \\ \\._,\\ '/ \n" +
                    "|____.'    `--'  `\"     |_|        |_|      `--'  `\"  \n" +
                    "                                       STARTED IN {} MS \n", System.currentTimeMillis() - startedTime);
        } catch (Exception e) {
            log.error("Exception during Jaffa RPC library startup:", e);
            throw new JaffaRpcSystemException(e);
        }
    }

    public void close() {
        log.info("Close started");
        this.kafkaReceivers.forEach(KafkaReceiver::close);
        log.info("Kafka receivers closed");
        KafkaRequestSender.shutDownConsumers();
        log.info("Kafka sync response consumers closed");
        if (Utils.conn != null) {
            try {
                for (String service : Utils.services) {
                    Utils.deleteAllRegistrations(service);
                }
                Utils.conn.close();
            } catch (KeeperException | InterruptedException | ParseException | UnknownHostException e) {
                log.error("Unable to unregister services from ZooKeeper cluster, probably it was done earlier");
            }
        }
        log.info("Services were unregistered");
        this.zmqReceivers.forEach(a -> {
            try {
                a.close();
            } catch (IOException e) {
                log.error("Unable to shut down ZeroMQ receivers", e);
                throw new JaffaRpcSystemException(e);
            }
        });
        ZContext context = ZeroMqRequestSender.context;
        if (!context.isClosed()) context.close();
        RabbitMQRequestSender.close();
        log.info("All ZMQ sockets were closed");
        for (Thread thread : this.receiverThreads) {
            do {
                thread.interrupt();
            } while (thread.getState() != Thread.State.TERMINATED);
        }
        log.info("All receiver threads stopped");
        FinalizationWorker.stopFinalizer();
        log.info("Finalizer was stopped");
        log.info("Jaffa RPC shutdown completed");
    }
}