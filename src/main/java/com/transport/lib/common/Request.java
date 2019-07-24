package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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

import static com.transport.lib.common.TransportService.*;
import static java.time.temporal.ChronoUnit.MINUTES;

@SuppressWarnings("all")
public class Request<T> implements RequestInterface<T> {

    private static Logger logger = LoggerFactory.getLogger(Request.class);

    // "object pool" of consumers that used for receiving sync response from server
    // each Request removes one of objects from consumers queue and puts back after timeout occurred of response was received
    private static final ConcurrentLinkedQueue<KafkaConsumer<String, byte[]>> consumers = new ConcurrentLinkedQueue<>();
    private final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
    private int timeout = -1;
    private String moduleId;
    private Command command;

    public Request(Command command) { this.command = command; }

    public static void initSyncKafkaConsumers(int brokersCount, CountDownLatch started) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        for (int i = 0; i < brokersCount; i++) {
            consumerProps.put("group.id", UUID.randomUUID().toString());
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consumers.add(consumer);
            started.countDown();
        }
    }

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
            throw new RuntimeException("No route for service: " + serviceInterface + " and module.id " + avaiableModuleId);
        else
            return topicName;
    }

    public Request<T> withTimeout(int timeout) {
        this.timeout = timeout;
        return this;
    }

    public Request<T> onModule(String moduleId) {
        this.moduleId = moduleId;
        return this;
    }

    // Only used when transport works through Kafka
    private byte[] waitForSyncAnswer(String requestTopic, long requestTime) {
        // Waiting for available consumer
        KafkaConsumer<String, byte[]> consumer;
        do {
            consumer = consumers.poll();
        } while (consumer == null);

        // we will wait for response message in "client" topic
        String clientTopicName = requestTopic.replace("-server", "-client");

        // 3 minute time offset to mitigate time desynchronization between client - kafka broker - server
        long threeMinAgo = Instant.ofEpochMilli(requestTime).minus(3, MINUTES).toEpochMilli();
        // resubscribe Kafka consumer to client target topic
        consumer.subscribe(Collections.singletonList(clientTopicName));
        // trigger consumer resubscription
        consumer.poll(0);
        // check metadata information for required client topic
        List<PartitionInfo> partitionInfos = consumer.listTopics().get(clientTopicName);
        List<TopicPartition> partitions = new ArrayList<>();
        if (partitionInfos == null) {
            logger.warn("Partition information was not found for topic ", clientTopicName);
        } else {
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition partition = new TopicPartition(clientTopicName, partitionInfo.partition());
                partitions.add(partition);
            }
        }
        // Seek consumer offset to call time minus 3 minutes
        // and start waiting for response from server
        Map<TopicPartition, Long> query = new HashMap<>();
        partitions.forEach(x -> query.put(x, threeMinAgo));
        Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);
        for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : result.entrySet()) {
            if (entry.getValue() == null) continue;
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

    @SuppressWarnings("unchecked")
    public T executeSync() {
        // Serialize command-request
        Kryo kryo = new Kryo();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        // Response from server, if null - transport timeout occurred
        byte[] response;
        if (Utils.useKafka()) {
            String requestTopic = getTopicForService(command.getServiceClass(), moduleId, true);
            try {
                ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(requestTopic, UUID.randomUUID().toString(), out.toByteArray());
                producer.send(resultPackage).get();
            } catch (Exception e) {
                logger.error("Error in sending sync request", e);
            }
            response = waitForSyncAnswer(requestTopic, System.currentTimeMillis());
        } else {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket socket = context.socket(ZMQ.REQ);
            socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.ZMQ));
            socket.send(out.toByteArray(), 0);
            if (timeout != -1) {
                socket.setReceiveTimeOut(timeout);
            }
            response = socket.recv(0);
            Utils.closeSocketAndContext(socket, context);
        }
        if (response == null) {
            throw new RuntimeException("Transport execution timeout");
        }
        Input input = new Input(new ByteArrayInputStream(response));
        Object result = kryo.readClassAndObject(input);
        input.close();
        if (result instanceof ExceptionHolder)
            throw new RuntimeException(((ExceptionHolder) result).getStackTrace());
        return (T) result;
    }

    public void executeAsync(String key, Class listener) {
        command.setCallbackClass(listener.getName());
        command.setCallbackKey(key);
        Kryo kryo = new Kryo();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        if (Utils.useKafka()) {
            try {
                ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(getTopicForService(command.getServiceClass(), moduleId, false), UUID.randomUUID().toString(), out.toByteArray());
                producer.send(resultPackage).get();
            } catch (Exception e) {
                logger.error("Error in sending async request", e);
            }
        } else {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket socket = context.socket(ZMQ.REQ);
            socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.ZMQ));
            socket.send(out.toByteArray(), 0);
            // Wait for "OK" message from server that means request was received and correctly deserialized
            socket.recv(0);
            Utils.closeSocketAndContext(socket, context);
        }
        // Add command to background finalization thread
        // that will throw "Transport execution timeout" on callback class after timeout expiration or 60 minutes if timeout was not set
        command.setAsyncExpireTime(System.currentTimeMillis() + (timeout != -1 ? timeout : 1000 * 60 * 60));
        logger.debug("Async command " + command + " added to finalization queue");
        FinalizationWorker.eventsToConsume.put(command.getCallbackKey(), command);
    }
}
