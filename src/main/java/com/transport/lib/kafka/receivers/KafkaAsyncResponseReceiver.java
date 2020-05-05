package com.transport.lib.kafka.receivers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.transport.lib.TransportService;
import com.transport.lib.common.FinalizationWorker;
import com.transport.lib.common.RebalanceListener;
import com.transport.lib.entities.CallbackContainer;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.ExceptionHolder;
import com.transport.lib.exception.TransportExecutionException;
import com.transport.lib.ui.AdminServer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;

import java.io.ByteArrayInputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class KafkaAsyncResponseReceiver extends KafkaReceiver implements Runnable {

    private final CountDownLatch countDownLatch;

    public KafkaAsyncResponseReceiver(CountDownLatch countDownLatch) {
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        TransportService.getConsumerProps().put("group.id", UUID.randomUUID().toString());
        Runnable consumerThread = () -> {
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(TransportService.getConsumerProps());
            Kryo kryo = new Kryo();
            consumer.subscribe(TransportService.getClientAsyncTopics(), new RebalanceListener());
            countDownLatch.countDown();
            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, byte[]> records = new ConsumerRecords<>(new HashMap<>());
                try {
                    records = consumer.poll(Duration.ofMillis(100));
                } catch (InterruptException ignore) {
                }
                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        Input input = new Input(new ByteArrayInputStream(record.value()));
                        CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                        Class<?> callbackClass = Class.forName(callbackContainer.getListener());
                        Command command = FinalizationWorker.eventsToConsume.remove(callbackContainer.getKey());
                        if (command != null) {
                            if (callbackContainer.getResult() instanceof ExceptionHolder) {
                                Method method = callbackClass.getMethod("onError", String.class, Throwable.class);
                                method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), new TransportExecutionException(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
                            } else {
                                Method method = callbackClass.getMethod("onSuccess", String.class, Class.forName(callbackContainer.getResultClass()));
                                if (Class.forName(callbackContainer.getResultClass()).equals(Void.class)) {
                                    method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), null);
                                } else
                                    method.invoke(callbackClass.getDeclaredConstructor().newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                            }
                            AdminServer.addMetric(command);
                        } else {
                            log.warn("Response {} already expired", callbackContainer.getKey());
                        }
                        Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                        commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        consumer.commitSync(commitData);
                    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException executionException) {
                        log.error("Error during receiving callback", executionException);
                        throw new TransportExecutionException(executionException);
                    }
                }
            }
            try {
                consumer.close();
            } catch (InterruptException ignore) {
            }
        };
        startThreadsAndWait(consumerThread);
    }
}
