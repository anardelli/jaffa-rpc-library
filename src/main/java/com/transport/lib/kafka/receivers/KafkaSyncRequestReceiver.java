package com.transport.lib.kafka.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.TransportService;
import com.transport.lib.common.RebalanceListener;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.RequestContext;
import com.transport.lib.exception.TransportSystemException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/*
    Class responsible for receiving sync requests using Kafka
 */
@Slf4j
public class KafkaSyncRequestReceiver extends KafkaReceiver implements Runnable {

    private final CountDownLatch countDownLatch;

    public KafkaSyncRequestReceiver(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        // Each RequestReceiver represents new group of consumers in Kafka
        TransportService.getConsumerProps().put("group.id", UUID.randomUUID().toString());
        Runnable consumerThread = () -> {
            // Each thread has consumer for receiving Requests
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(TransportService.getConsumerProps());
            // And producer for sending invocation results or CallbackContainers (for async calls)
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(TransportService.getProducerProps());
            // Then we subscribe to known server topics and waiting for requests
            consumer.subscribe(TransportService.getServerSyncTopics(), new RebalanceListener());
            // New Kryo instance per thread
            Kryo kryo = new Kryo();
            // Here we consider receiver thread as started
            countDownLatch.countDown();
            // Waiting and processing requests
            while (!Thread.currentThread().isInterrupted()) {
                // Wait data for 100 ms if no new records available after last committed
                ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(new HashMap<>());
                try {
                    records = consumer.poll(Duration.ofMillis(100));
                } catch (InterruptException ignore) {
                }
                // Process requests
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        // Each request is represented by Command instance
                        Input input = new Input(new ByteArrayInputStream(record.value()));
                        // Deserialize Command from byte[]
                        Command command = kryo.readObject(input, Command.class);
                        // Target method will be executed in current Thread, so set service metadata
                        // like client's module.id and SecurityTicket token in ThreadLocal variables
                        RequestContext.setSourceModuleId(command.getSourceModuleId());
                        RequestContext.setSecurityTicket(command.getTicket());
                        // Invoke target method and receive result
                        Object result = TransportService.invoke(command);
                        // Prepare for result marshalling
                        ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                        Output output = new Output(bOutput);
                        // Marshall result
                        kryo.writeClassAndObject(output, TransportService.getResult(result));
                        output.close();
                        // Prepare record with result. Here we construct topic name on the fly
                        ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(command.getServiceClass().replace("Transport", "") + "-" + TransportService.getRequiredOption("module.id") + "-client-sync", command.getRqUid(), bOutput.toByteArray());
                        // Send record and ignore returned RecordMetadata
                        producer.send(resultPackage).get();
                        // Commit original request's message
                        Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                        commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        consumer.commitSync(commitData);
                    } catch (ExecutionException | InterruptedException executionException) {
                        log.error("Target method execution exception", executionException);
                        throw new TransportSystemException(executionException);
                    }
                }
            }
            try {
                consumer.close();
                producer.close();
            } catch (InterruptException ignore) {
            }
        };
        // Start receiver threads
        startThreadsAndWait(consumerThread);
    }
}
