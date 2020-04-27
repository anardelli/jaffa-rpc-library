package com.transport.lib.kafka;

import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.request.RequestUtils;
import com.transport.lib.request.Sender;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.transport.lib.TransportService.getRequiredOption;
import static com.transport.lib.TransportService.producerProps;
import static java.time.temporal.ChronoUnit.MINUTES;

public class KafkaRequestSender extends Sender {

    // Object pool of consumers that used for receiving sync response from server
    // each Request removes one of objects from consumers queue and puts back after timeout occurred of response was received
    private static final ConcurrentLinkedQueue<KafkaConsumer<String, byte[]>> consumers = new ConcurrentLinkedQueue<>();
    private static Logger logger = LoggerFactory.getLogger(KafkaRequestSender.class);
    // One producer per Request - we need to optimize it
    private final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);

    // Initialize Consumer object pool
    public static void initSyncKafkaConsumers(int brokersCount, CountDownLatch started) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        // Earliest offset by default, but it will be set manually lately
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        for (int i = 0; i < brokersCount; i++) {
            // Every Consumer represents unique consumer group
            consumerProps.put("group.id", UUID.randomUUID().toString());
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consumers.add(consumer);
            started.countDown();
        }
    }

    public static void shutDownConsumers(){
        consumers.forEach(KafkaConsumer::close);
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
        final KafkaConsumer<String, byte[]> finalConsumer = consumer;
        consumer.subscribe(Collections.singletonList(clientTopicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) { }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                // Seek consumer offset to call time minus 3 minutes and start waiting for response from server
                Map<TopicPartition, Long> query = new HashMap<>();
                partitions.forEach(x -> query.put(x, threeMinAgo));
                for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : finalConsumer.offsetsForTimes(query).entrySet()) {
                    if (entry.getValue() == null) continue;
                    // Apply offsets to consumer
                    finalConsumer.seek(entry.getKey(), entry.getValue().offset());
                }
            }
        });
        /*
            Start timer for timeout.
            Timeout could be set by used or default == 60 minutes
            Timeout occurred, stop waiting and return null
         */
        long start = System.currentTimeMillis();
        while (!((timeout != -1 && System.currentTimeMillis() - start > timeout) || (System.currentTimeMillis() - start > (1000 * 60 * 60)))) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(10));
            for (ConsumerRecord<String, byte[]> record : records) {
                // Response must has same key as request
                if (record.key().equals(command.getRqUid())) {
                    try {
                        Map<TopicPartition, OffsetAndMetadata> commits = new HashMap<>();
                        commits.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        // Manually commit that message
                        consumer.commitSync(commits);
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

    @Override
    public byte[] executeSync(byte[] message) {
        // Get server-side async topic in Kafka
        String requestTopic = RequestUtils.getTopicForService(command.getServiceClass(), moduleId, true);
        try {
            // Prepare message with random key and byte[] payload
            ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(requestTopic, UUID.randomUUID().toString(), message);
            // Send message and ignore RecordMetadata
            producer.send(resultPackage).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error in sending sync request", e);
            // Kafka cluster is broken, return exception to user
            throw new TransportExecutionException(e);
        }
        // Waiting for answer from server
        return waitForSyncAnswer(requestTopic, System.currentTimeMillis());
    }

    @Override
    public void executeAsync(byte[] message) {
        try {
            // Prepare message with random key and byte[] payload
            ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(RequestUtils.getTopicForService(command.getServiceClass(), moduleId, false), UUID.randomUUID().toString(), message);
            // Send message to server-side sync topic and ignore RecordMetadata
            producer.send(resultPackage).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error in sending async request", e);
            // Kafka cluster is broken, return exception to user
            throw new TransportExecutionException(e);
        }
    }
}
