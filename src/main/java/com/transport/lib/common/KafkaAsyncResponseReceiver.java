package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.transport.lib.common.TransportService.*;

@SuppressWarnings("WeakerAccess, unchecked")
public class KafkaAsyncResponseReceiver extends KafkaReceiver implements Runnable {

    private static Logger logger = LoggerFactory.getLogger(KafkaAsyncResponseReceiver.class);

    private CountDownLatch countDownLatch;

    public KafkaAsyncResponseReceiver(CountDownLatch countDownLatch){
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        consumerProps.put("group.id", UUID.randomUUID().toString());
        Runnable consumerThread = () ->  {
            try{
                KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
                consumer.subscribe(clientAsyncTopics, new RebalanceListener());
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
                        } catch (Exception executionException) {
                            logger.error("Error during receiving callback", executionException);
                        }
                    }
                }
            }catch (InterruptException ignore){
            }catch (Exception generalKafkaException){
                logger.error("General Kafka exception", generalKafkaException);
            }
        };
        startThreadsAndWait(consumerThread);
    }
}
