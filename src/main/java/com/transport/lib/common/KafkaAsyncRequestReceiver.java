package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
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
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.transport.lib.common.TransportService.*;

/*
    Class responsible for receiving async requests using Kafka
 */
public class KafkaAsyncRequestReceiver extends KafkaReceiver implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(KafkaAsyncRequestReceiver.class);

    private CountDownLatch countDownLatch;

    KafkaAsyncRequestReceiver(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        // Each RequestReceiver represents new group of consumers in Kafka
        consumerProps.put("group.id", UUID.randomUUID().toString());
        // Start <number of brokers> consumers
        Runnable consumerThread = () -> {
            try {
                // Each thread has consumer for receiving Requests
                KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
                // And producer for sending invocation results or CallbackContainers (for async calls)
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
                // New Kryo instance per thread
                Kryo kryo = new Kryo();
                // Then we subscribe to known server topics and waiting for requests
                consumer.subscribe(serverAsyncTopics, new RebalanceListener());
                // Here we consider receiver thread as started
                countDownLatch.countDown();
                // Waiting and processing requests
                while (!Thread.currentThread().isInterrupted()) {
                    // Wait data for 100 ms if no new records available after last committed
                    ConsumerRecords<String, byte[]> records = consumer.poll(100);
                    // Process requests
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            // Each request is represented by Command instance
                            Input input = new Input(new ByteArrayInputStream(record.value()));
                            // Deserialize Command from byte[]
                            Command command = kryo.readObject(input, Command.class);
                            // Target method will be executed in current Thread, so set service metadata
                            // like client's module.id and SecurityTicket token in ThreadLocal variables
                            TransportContext.setSourceModuleId(command.getSourceModuleId());
                            TransportContext.setSecurityTicketThreadLocal(command.getTicket());
                            // Invoke target method and receive result
                            Object result = invoke(command);
                            // Marshall result as CallbackContainer
                            ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                            Output output = new Output(bOutput);
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
                            // Write object to output stream
                            kryo.writeObject(output, callbackContainer);
                            output.close();
                            // Prepare record with result. Here we construct topic name on the fly
                            ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(command.getServiceClass().replace("Transport", "") + "-" + command.getSourceModuleId() + "-client-async", UUID.randomUUID().toString(), bOutput.toByteArray());
                            // Send record and ignore returned RecordMetadata
                            producer.send(resultPackage).get();
                            // Commit original request's message
                            Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                            commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                            consumer.commitSync(commitData);
                        } catch (Exception executionException) {
                            logger.error("Target method execution exception", executionException);
                        }
                    }
                }
            } catch (InterruptException ignore) {
            } catch (Exception generalKafkaException) {
                logger.error("General Kafka exception", generalKafkaException);
            }
        };
        // Start receiver threads
        startThreadsAndWait(consumerThread);
    }
}
