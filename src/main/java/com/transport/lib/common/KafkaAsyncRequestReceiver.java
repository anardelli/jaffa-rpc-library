package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import kafka.admin.RackAwareMode;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.reflections.Reflections;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.transport.lib.common.TransportService.*;

@SuppressWarnings("WeakerAccess, unchecked")
public class KafkaAsyncRequestReceiver implements Runnable {

    private CountDownLatch countDownLatch;

    private static final ArrayList<Thread> serverConsumers = new ArrayList<>(brokersCount);

    public KafkaAsyncRequestReceiver(CountDownLatch countDownLatch){
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        consumerProps.put("group.id", UUID.randomUUID().toString());
        Runnable consumerThread = () ->  {
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            KafkaProducer<String,byte[]> producer = new KafkaProducer<>(producerProps);
            consumer.subscribe(serverAsyncTopics, new RebalanceListener());
            countDownLatch.countDown();
            while(!Thread.currentThread().isInterrupted()){
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for(ConsumerRecord<String,byte[]> record: records){
                    try {
                        Kryo kryo = new Kryo();
                        Input input = new Input(new ByteArrayInputStream(record.value()));
                        Command command = kryo.readObject(input, Command.class);
                        Object result = invoke(command);
                        ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                        Output output = new Output(bOutput);
                        CallbackContainer callbackContainer = new CallbackContainer();
                        callbackContainer.setKey(command.getCallbackKey());
                        callbackContainer.setListener(command.getCallbackClass());
                        callbackContainer.setResult(getResult(result));
                        Method targetMethod = getTargetMethod(command);
                        if (map.containsKey(targetMethod.getReturnType())) {
                            callbackContainer.setResultClass(map.get(targetMethod.getReturnType()).getName());
                        } else {
                            callbackContainer.setResultClass(targetMethod.getReturnType().getName());
                        }
                        kryo.writeObject(output, callbackContainer);
                        output.close();
                        ProducerRecord<String,byte[]> resultPackage = new ProducerRecord<>(command.getServiceClass().replace("Transport", "") + "-" + command.getSourceModuleId() + "-client-async", UUID.randomUUID().toString(), bOutput.toByteArray());
                        producer.send(resultPackage).get();
                        Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                        commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        consumer.commitSync(commitData);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
            }
        };
        for(int i = 0; i < brokersCount; i++){
            serverConsumers.add(new Thread(consumerThread));
        }
        serverConsumers.forEach(Thread::start);
        serverConsumers.forEach(x -> {try{x.join();} catch (Exception e){e.printStackTrace();}});
    }
}
