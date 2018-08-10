package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import kafka.admin.RackAwareMode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.reflections.Reflections;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.transport.lib.common.TransportService.*;

@SuppressWarnings("WeakerAccess, unchecked")
public class KafkaAsyncResponseReceiver implements Runnable {

    private static final ArrayList<Thread> clientConsumers = new ArrayList<>(brokersCount);

    private CountDownLatch countDownLatch;

    public KafkaAsyncResponseReceiver(CountDownLatch countDownLatch){
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        consumerProps.put("group.id", UUID.randomUUID().toString());
        Runnable consumerThread = () ->  {
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(clientAsyncTopics);
            countDownLatch.countDown();
            while(!Thread.currentThread().isInterrupted()){
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for(ConsumerRecord<String,byte[]> record: records){
                    try {
                        Kryo kryo = new Kryo();
                        Input input = new Input(new ByteArrayInputStream(record.value()));
                        CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
                        Class callbackClass = Class.forName(callbackContainer.getListener());
                        if(callbackContainer.getResult() instanceof ExceptionHolder) {
                            Method method = callbackClass.getMethod("callBackError", String.class, Throwable.class );
                            method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), new Throwable(((ExceptionHolder) callbackContainer.getResult()).getStackTrace()));
                        }else {
                            Method method = callbackClass.getMethod("callBack", String.class, Class.forName(callbackContainer.getResultClass()));
                            if(Class.forName(callbackContainer.getResultClass()).equals(Void.class)){
                                method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), null);
                            }else
                                method.invoke(callbackClass.newInstance(), callbackContainer.getKey(), callbackContainer.getResult());
                        }
                        Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                        commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        consumer.commitSync(commitData);
                    } catch (Exception e) {
                        System.out.println("Error during receiving callback:");
                        e.printStackTrace();
                    }
                }
            }
        };
        for(int i = 0; i < brokersCount; i++){
            clientConsumers.add(new Thread(consumerThread));
        }
        clientConsumers.forEach(Thread::start);
        clientConsumers.forEach(x -> {try{x.join();} catch (Exception e){e.printStackTrace();}});
    }
}
