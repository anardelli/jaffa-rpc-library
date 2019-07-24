package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.transport.lib.common.TransportService.clientAsyncTopics;
import static com.transport.lib.common.TransportService.consumerProps;

@SuppressWarnings("WeakerAccess, unchecked")
public class KafkaAsyncResponseReceiver extends KafkaReceiver implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(KafkaAsyncResponseReceiver.class);

    // Used for waiting receiver thread startup
    private CountDownLatch countDownLatch;

    public KafkaAsyncResponseReceiver(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        // New group.id per consumer group
        consumerProps.put("group.id", UUID.randomUUID().toString());
        Runnable consumerThread = () -> {
            try {
                // Consumer waiting for async responses (CallbackContainer) from server
                KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
                // Subscribe to known client topics
                consumer.subscribe(clientAsyncTopics, new RebalanceListener());
                countDownLatch.countDown();
                while (!Thread.currentThread().isInterrupted()) {
                    // Wait data for 100 ms if no new records available after last committed
                    ConsumerRecords<String, byte[]> records = consumer.poll(100);
                    // Process CallbackContainers
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            // Deserialize response
                            Kryo kryo = new Kryo();
                            Input input = new Input(new ByteArrayInputStream(record.value()));
                            CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                            // Take target callback class
                            Class callbackClass = Class.forName(callbackContainer.getListener());
                            // If timeout not occurred yet - process response and invoke target method
                            if (FinalizationWorker.eventsToConsume.remove(callbackContainer.getKey()) != null) {
                                // Exception occurred on server side
                                if (callbackContainer.getResult() instanceof ExceptionHolder) {
                                    // Invoke onError with message from ExceptionHolder
                                    Method method = callbackClass.getMethod("onError", String.class, Throwable.class);
                                    method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), new Throwable(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
                                } else {
                                    // Invoke onSuccess
                                    Method method = callbackClass.getMethod("onSuccess", String.class, Class.forName(callbackContainer.getResultClass()));
                                    // If target method return type is void, then result object is null
                                    if (Class.forName(callbackContainer.getResultClass()).equals(Void.class)) {
                                        method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), null);
                                    } else
                                        method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                                }
                            } else {
                                // Server failed to respond in time and invocation was already finalized with "Transport execution timeout"
                                logger.warn("Response " + callbackContainer.getKey() + " already expired");
                            }
                            // Manually commit offsets for processed responses
                            Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                            commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                            consumer.commitSync(commitData);
                        } catch (Exception executionException) {
                            logger.error("Error during receiving callback", executionException);
                        }
                    }
                }
            } catch (InterruptException ignore) {
            } catch (Exception generalKafkaException) {
                logger.error("General Kafka exception", generalKafkaException);
            }
        };
        startThreadsAndWait(consumerThread);
    }
}
