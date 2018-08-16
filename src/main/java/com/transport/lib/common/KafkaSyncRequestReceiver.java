package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.transport.lib.common.TransportService.*;

@SuppressWarnings("WeakerAccess, unchecked")
public class KafkaSyncRequestReceiver implements Runnable {

    private static final ArrayList<Thread> serverConsumers = new ArrayList<>(brokersCount);

    private CountDownLatch countDownLatch;

    public KafkaSyncRequestReceiver(CountDownLatch countDownLatch){
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("group.id", UUID.randomUUID().toString());
        Runnable consumerThread = () ->  {
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            KafkaProducer<String,byte[]> producer = new KafkaProducer<>(producerProps);
            consumer.subscribe(serverSyncTopics, new RebalanceListener());
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
                        kryo.writeClassAndObject(output, getResult(result));
                        output.close();
                        ProducerRecord<String,byte[]> resultPackage = new ProducerRecord<>(command.getServiceClass().replace("Transport", "") + "-" + getRequiredOption("module.id") + "-client-sync", command.getRqUid(), bOutput.toByteArray());
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
