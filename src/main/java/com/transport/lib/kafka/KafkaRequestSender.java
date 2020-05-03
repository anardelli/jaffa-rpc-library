package com.transport.lib.kafka;

import com.transport.lib.TransportService;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.request.RequestUtils;
import com.transport.lib.request.Sender;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static java.time.temporal.ChronoUnit.MINUTES;

@Slf4j
public class KafkaRequestSender extends Sender {

    // Object pool of consumers that used for receiving sync response from server
    // each Request removes one of objects from consumers queue and puts back after timeout occurred of response was received
    private static final ConcurrentLinkedQueue<KafkaConsumer<String, byte[]>> consumers = new ConcurrentLinkedQueue<>();
    // One producer per Request - we need to optimize it
    private final KafkaProducer<String, byte[]> producer = new KafkaProducer<>(TransportService.getProducerProps());

    // Initialize Consumer object pool
    public static void initSyncKafkaConsumers(int brokersCount, CountDownLatch started) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", TransportService.getRequiredOption("bootstrap.servers"));
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

    public static void shutDownConsumers() {
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
        long startRebalance = System.nanoTime();
        consumer.subscribe(Collections.singletonList(clientTopicName), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                // No-op
            }

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
                log.info(">>>>>> Partitions assigned took {} ns", System.nanoTime() - startRebalance);
            }
        });
        log.info(">>>>>> Consumer rebalance took {} ns", System.nanoTime() - startRebalance);
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
                        log.error("Error during commit received answer", e);
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
        long start = System.currentTimeMillis();
        // Get server-side async topic in Kafka
        String requestTopic = RequestUtils.getTopicForService(command.getServiceClass(), moduleId, true);
        try {
            // Prepare message with random key and byte[] payload
            ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(requestTopic, UUID.randomUUID().toString(), message);
            // Send message and ignore RecordMetadata
            producer.send(resultPackage).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error in sending sync request", e);
            // Kafka cluster is broken, return exception to user
            throw new TransportExecutionException(e);
        }
        // Waiting for answer from server
        byte[] result = waitForSyncAnswer(requestTopic, System.currentTimeMillis());
        log.info(">>>>>> Executed sync request {} in {} ms", command.getRqUid(), System.currentTimeMillis() - start);
        return result;
    }

    @Override
    public void executeAsync(byte[] message) {
        long start = System.currentTimeMillis();
        try {
            // Prepare message with random key and byte[] payload
            ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(RequestUtils.getTopicForService(command.getServiceClass(), moduleId, false), UUID.randomUUID().toString(), message);
            // Send message to server-side sync topic and ignore RecordMetadata
            producer.send(resultPackage).get();
            log.info(">>>>>> Executed async request {} in {} ms", command.getRqUid(), System.currentTimeMillis() - start);
        } catch (InterruptedException | ExecutionException e) {
            log.error("Error while sending async Kafka request", e);
            // Kafka cluster is broken, return exception to user
            throw new TransportExecutionException(e);
        }
    }
}
