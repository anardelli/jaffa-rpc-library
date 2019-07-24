package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportExecutionTimeoutException;
import com.transport.lib.exception.TransportNoRouteException;
import com.transport.lib.zookeeper.Utils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.transport.lib.common.TransportService.*;
import static java.time.temporal.ChronoUnit.MINUTES;

/*
    Class responsible for making synchronous and asynchronous requests
 */
@SuppressWarnings("all")
public class Request<T> implements RequestInterface<T> {

    private static Logger logger = LoggerFactory.getLogger(Request.class);

    // Object pool of consumers that used for receiving sync response from server
    // each Request removes one of objects from consumers queue and puts back after timeout occurred of response was received
    private static final ConcurrentLinkedQueue<KafkaConsumer<String, byte[]>> consumers = new ConcurrentLinkedQueue<>();
    // One producer per Request - we need to optimize it
    private final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
    // Time period in milliseconds during which we wait for answer from server
    private int timeout = -1;
    // Target module.id
    private String moduleId;
    // Target command
    private Command command;
    // New Kryo instance per thread
    private Kryo kryo = new Kryo();

    public Request(Command command) { this.command = command; }

    // Initialize Consumer object pool
    public static void initSyncKafkaConsumers(int brokersCount, CountDownLatch started) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        // Earlist offset by default, but it will be set manually lately
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        for (int i = 0; i < brokersCount; i++) {
            // Every Consumer represents unique consumer group
            consumerProps.put("group.id", UUID.randomUUID().toString());
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consumers.add(consumer);
            started.countDown();
        }
    }

    /*
        Returns server-side topic name for synchronous and asynchronous requests
        Topic name looks like <class name with packages>-<module.id>-server-<sync>/<async>
        - Fully-qualified target class name defined in Command
        - module.id could be provided by user or discovered from currently available in cluster
        - If no API implementations currently available or module is registered
          but his server topic does not exists - RuntimeException will be thrown
     */
    private static String getTopicForService(String service, String moduleId, boolean sync) {
        String serviceInterface = service.replace("Transport", "");
        String avaiableModuleId = moduleId;
        if (moduleId != null) {
            // Checks for active server for a given service with specific module id
            // or throws "transport no route" exception if there are none
            Utils.getHostForService(serviceInterface, moduleId, Protocol.KAFKA);
        } else {
            // if moduleId  was not specified - get module id of any active server for a given service
            // or throws "transport no route" exception if there are none
            avaiableModuleId = Utils.getModuleForService(serviceInterface, Protocol.KAFKA);
        }
        String topicName = serviceInterface + "-" + avaiableModuleId + "-server" + (sync ? "-sync" : "-async");
        // if necessary topic does not exist for some reason - throw "transport no route" exception
        if (!zkClient.topicExists(topicName))
            throw new TransportNoRouteException(serviceInterface, avaiableModuleId);
        else
            return topicName;
    }

    /*
        Setter for user-provided timeout
     */
    public Request<T> withTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    /*
        Setter for user-provided server module.id
     */
    public Request<T> onModule(String moduleId) {
        this.moduleId = moduleId;
        return this;
    }

    /*
        Only used when transport works using Kafka
        Waits for message with specific key in topic for a given period of time in milliseconds
     */
    private byte[] waitForSyncAnswer(String requestTopic, long requestTime) {
        // Waiting for available consumer
        KafkaConsumer<String, byte[]> consumer;
        do {
            consumer = consumers.poll();
        } while (consumer == null);

        // We will wait for response message in "client" topic
        String clientTopicName = requestTopic.replace("-server", "-client");

        // 3 minute time offset to mitigate time desynchronization between client - kafka broker - server
        long threeMinAgo = Instant.ofEpochMilli(requestTime).minus(3, MINUTES).toEpochMilli();
        // Resubscribe Kafka consumer to client target topic
        consumer.subscribe(Collections.singletonList(clientTopicName));
        // Trigger consumer resubscription
        consumer.poll(0);
        // Check metadata information for required client topic
        List<PartitionInfo> partitionInfos = consumer.listTopics().get(clientTopicName);
        List<TopicPartition> partitions = new ArrayList<>();
        // No metadata - topic doesn't exist
        if (partitionInfos == null) {
            logger.error("Partition information was not found for topic ", clientTopicName);
        } else {
            // Collect metadata about all partitions for client topic
            partitions = partitionInfos.stream().map(x -> new TopicPartition(clientTopicName, x.partition())).collect(Collectors.toList());
        }
        // Seek consumer offset to call time minus 3 minutes and start waiting for response from server
        Map<TopicPartition, Long> query = new HashMap<>();
        partitions.forEach(x -> query.put(x, threeMinAgo));
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : consumer.offsetsForTimes(query).entrySet()) {
            if (entry.getValue() == null) continue;
            // Apply offsets to consumer
            consumer.seek(entry.getKey(), entry.getValue().offset());
        }

        // Start timer for timeout.
        // Timeout could be set by used or default == 60 minutes
        long start = System.currentTimeMillis();
        while (true) {
            // Timeout occurred, stop waiting and return null
            if ((timeout != -1 && System.currentTimeMillis() - start > timeout) || (System.currentTimeMillis() - start > (1000 * 60 * 60)))
                break;
            ConsumerRecords<String, byte[]> records = consumer.poll(10);
            for (ConsumerRecord<String, byte[]> record : records) {
                // Response must has same key as request
                if (record.key().equals(command.getRqUid())) {
                    try {
                        Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                        commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        // Manually commit that message
                        consumer.commitSync(commitData);
                    } catch (CommitFailedException e) {
                        logger.error("Error during commit received answer", e);
                    }
                    // Return consumer to object pool
                    consumers.add(consumer);
                    return record.value();
                }
            }
        }
        // Return consumer to object pool
        consumers.add(consumer);
        return null;
    }

    /*
        Responsible for making synchronous request and waiting for answer using Kafka or ZeroMQ
     */
    public T executeSync() {
        // Serialize command-request
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        // Response from server, if null - transport timeout occurred
        byte[] response;
        if (Utils.useKafka()) {
            // Get server-side async topic in Kafka
            String requestTopic = getTopicForService(command.getServiceClass(), moduleId, true);
            try {
                // Prepare message with random key and byte[] payload
                ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(requestTopic, UUID.randomUUID().toString(), out.toByteArray());
                // Send message and ignore RecordMetadata
                producer.send(resultPackage).get();
            } catch (Exception e) {
                logger.error("Error in sending sync request", e);
                // Kafka cluster is broken, return exception to user
                throw new TransportExecutionException(e);
            }
            // Waiting for asnwer from server
            response = waitForSyncAnswer(requestTopic, System.currentTimeMillis());
        } else {
            // New ZeroMQ context with 1 thread
            ZMQ.Context context = ZMQ.context(1);
            // Open socket
            ZMQ.Socket socket = context.socket(ZMQ.REQ);
            // Get target server host:port
            socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.ZMQ));
            // Send Command to server
            socket.send(out.toByteArray(), 0);
            // Set timeout if provided
            if (timeout != -1) {
                socket.setReceiveTimeOut(timeout);
            }
            // Wait for answer from server
            response = socket.recv(0);
            // Close socket and context
            Utils.closeSocketAndContext(socket, context);
        }
        // Response could be null ONLY of timeout occurred, otherwise it is object or void or ExceptionHolder
        if (response == null) {
            throw new TransportExecutionTimeoutException();
        }
        Input input = new Input(new ByteArrayInputStream(response));
        Object result = kryo.readClassAndObject(input);
        input.close();
        // Server returned ExceptionHolder - exception occurred on server side
        if (result instanceof ExceptionHolder)
            throw new TransportExecutionException(((ExceptionHolder) result).getStackTrace());
        return (T) result;
    }

    /*
        Responsible for making asynchronous request using Kafka or ZeroMQ
     */
    public void executeAsync(String key, Class listener) {
        // Set Callback class name
        command.setCallbackClass(listener.getName());
        // Set user-provided unique callback key
        command.setCallbackKey(key);
        // Marshall Command using Kryo
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        // Send Request using Kafka or ZeroMQ
        if (Utils.useKafka()) {
            try {
                // Prepare message with random key and byte[] payload
                ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(getTopicForService(command.getServiceClass(), moduleId, false), UUID.randomUUID().toString(), out.toByteArray());
                // Send message to server-side sync topic and ignore RecordMetadata
                producer.send(resultPackage).get();
            } catch (Exception e) {
                logger.error("Error in sending async request", e);
                // Kafka cluster is broken, return exception to user
                throw new TransportExecutionException(e);
            }
        } else {
            // New ZeroMQ context with 1 thread
            ZMQ.Context context = ZMQ.context(1);
            // Open socket
            ZMQ.Socket socket = context.socket(ZMQ.REQ);
            // Get target server host:port
            socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.ZMQ));
            // Send Command to server
            socket.send(out.toByteArray(), 0);
            // Wait for "OK" message from server that means request was received and correctly deserialized
            socket.recv(0);
            Utils.closeSocketAndContext(socket, context);
        }
        // Add command to background finalization thread
        // that will throw "Transport execution timeout" on callback class after timeout expiration or 60 minutes if timeout was not set
        command.setAsyncExpireTime(System.currentTimeMillis() + (timeout != -1 ? timeout : 1000 * 60 * 60));
        logger.debug("Async command " + command + " added to finalization queue");
        // Add Command to finalization queue
        FinalizationWorker.eventsToConsume.put(command.getCallbackKey(), command);
    }
}
