package com.transport.lib.kafka.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.TransportService;
import com.transport.lib.common.RebalanceListener;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.RequestContext;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.exception.TransportSystemException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

/*
    Class responsible for receiving async requests using Kafka
 */
public class KafkaAsyncRequestReceiver extends KafkaReceiver implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAsyncRequestReceiver.class);

    private final CountDownLatch countDownLatch;

    public KafkaAsyncRequestReceiver(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        // Each RequestReceiver represents new group of consumers in Kafka
        TransportService.getConsumerProps().put("group.id", UUID.randomUUID().toString());
        // Start <number of brokers> consumers
        Runnable consumerThread = () -> {
            // Each thread has consumer for receiving Requests
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(TransportService.getConsumerProps());
            // And producer for sending invocation results or CallbackContainers (for async calls)
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(TransportService.getProducerProps());
            // New Kryo instance per thread
            Kryo kryo = new Kryo();
            // Then we subscribe to known server topics and waiting for requests
            consumer.subscribe(TransportService.getServerAsyncTopics(), new RebalanceListener());
            // Here we consider receiver thread as started
            countDownLatch.countDown();
            // Waiting and processing requests
            while (!Thread.currentThread().isInterrupted()) {
                // Wait data for 100 ms if no new records available after last committed
                ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(new HashMap<>());
                try{
                    records = consumer.poll(Duration.ofMillis(100));
                }catch (InterruptException ignore){}
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
                        // Marshall result as CallbackContainer
                        ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                        Output output = new Output(bOutput);
                        // Construct CallbackContainer and marshall it to output stream
                        kryo.writeObject(output, TransportService.constructCallbackContainer(command, result));
                        output.close();
                        // Prepare record with result. Here we construct topic name on the fly
                        ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(command.getServiceClass().replace("Transport", "") + "-" + command.getSourceModuleId() + "-client-async", UUID.randomUUID().toString(), bOutput.toByteArray());
                        // Send record and ignore returned RecordMetadata
                        producer.send(resultPackage).get();
                        // Commit original request's message
                        Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                        commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        consumer.commitSync(commitData);
                    } catch (ClassNotFoundException | NoSuchMethodException executionException) {
                        logger.error("Target method execution exception", executionException);
                        throw new TransportExecutionException(executionException);
                    } catch (InterruptedException | ExecutionException systemException) {
                        logger.error("General Kafka exception", systemException);
                        throw new TransportSystemException(systemException);
                    }
                }
            }
            try {
                consumer.close();
                producer.close();
            }catch (InterruptException ignore){}
        };
        // Start receiver threads
        startThreadsAndWait(consumerThread);
    }
}
