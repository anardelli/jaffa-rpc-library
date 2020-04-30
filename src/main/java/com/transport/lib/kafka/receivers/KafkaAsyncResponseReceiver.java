package com.transport.lib.kafka.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.transport.lib.common.FinalizationWorker;
import com.transport.lib.common.RebalanceListener;
import com.transport.lib.entities.CallbackContainer;
import com.transport.lib.entities.ExceptionHolder;
import com.transport.lib.exception.TransportExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.transport.lib.TransportService.clientAsyncTopics;
import static com.transport.lib.TransportService.consumerProps;

/*
    Class responsible for receiving async responses using Kafka
 */
public class KafkaAsyncResponseReceiver extends KafkaReceiver implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(KafkaAsyncResponseReceiver.class);

    // Used for waiting receiver thread startup
    private final CountDownLatch countDownLatch;

    public KafkaAsyncResponseReceiver(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        // New group.id per consumer group
        consumerProps.put("group.id", UUID.randomUUID().toString());
        Runnable consumerThread = () -> {
            // Consumer waiting for async responses (CallbackContainer) from server
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            // New Kryo instance per thread
            Kryo kryo = new Kryo();
            // Subscribe to known client topics
            consumer.subscribe(clientAsyncTopics, new RebalanceListener());
            countDownLatch.countDown();
            while (!Thread.currentThread().isInterrupted()) {
                // Wait data for 100 ms if no new records available after last committed
                ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(new HashMap<>());
                try {
                    records = consumer.poll(Duration.ofMillis(100));
                }catch (InterruptException ignore){}
                // Process CallbackContainers
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        // Deserialize response
                        Input input = new Input(new ByteArrayInputStream(record.value()));
                        CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                        // Take target callback class
                        Class<?> callbackClass = Class.forName(callbackContainer.getListener());
                        // If timeout not occurred yet - process response and invoke target method
                        if (FinalizationWorker.eventsToConsume.remove(callbackContainer.getKey()) != null) {
                            // Exception occurred on server side
                            if (callbackContainer.getResult() instanceof ExceptionHolder) {
                                // Invoke onError with message from ExceptionHolder
                                Method method = callbackClass.getMethod("onError", String.class, Throwable.class);
                                method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), new TransportExecutionException(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
                            } else {
                                // Invoke onSuccess
                                Method method = callbackClass.getMethod("onSuccess", String.class, Class.forName(callbackContainer.getResultClass()));
                                // If target method return type is void, then result object is null
                                if (Class.forName(callbackContainer.getResultClass()).equals(Void.class)) {
                                    method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), null);
                                } else
                                    method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                            }
                        } else {
                            // Server failed to respond in time and invocation was already finalized with "Transport execution timeout"
                            logger.warn("Response {} already expired", callbackContainer.getKey());
                        }
                        // Manually commit offsets for processed responses
                        Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                        commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        consumer.commitSync(commitData);
                    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException executionException) {
                        logger.error("Error during receiving callback", executionException);
                        throw new TransportExecutionException(executionException);
                    }
                }
            }
            try {
                consumer.close();
            }catch (InterruptException ignore){}
        };
        startThreadsAndWait(consumerThread);
    }
}
