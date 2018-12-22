package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.concurrent.CountDownLatch;

import static com.transport.lib.common.TransportService.*;

@SuppressWarnings("WeakerAccess")
public class KafkaSyncRequestReceiver extends KafkaReceiver implements Runnable{

    private static Logger logger = LoggerFactory.getLogger(KafkaSyncRequestReceiver.class);

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
            try {
                KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
                KafkaProducer<String, byte[]> producer = new KafkaProducer<>(producerProps);
                consumer.subscribe(serverSyncTopics, new RebalanceListener());
                countDownLatch.countDown();
                while (!Thread.currentThread().isInterrupted()) {
                    ConsumerRecords<String, byte[]> records = consumer.poll(100);
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            Kryo kryo = new Kryo();
                            Input input = new Input(new ByteArrayInputStream(record.value()));
                            Command command = kryo.readObject(input, Command.class);
                            TransportContext.setSourceModuleId(command.getSourceModuleId());
                            TransportContext.setSecurityTicketThreadLocal(command.getTicket());
                            Object result = invoke(command);
                            ByteArrayOutputStream bOutput = new ByteArrayOutputStream();
                            Output output = new Output(bOutput);
                            kryo.writeClassAndObject(output, getResult(result));
                            output.close();
                            ProducerRecord<String, byte[]> resultPackage = new ProducerRecord<>(command.getServiceClass().replace("Transport", "") + "-" + getRequiredOption("module.id") + "-client-sync", command.getRqUid(), bOutput.toByteArray());
                            producer.send(resultPackage).get();
                            Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                            commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                            consumer.commitSync(commitData);
                        } catch (Exception executionException) {
                            logger.error("Target method execution exception", executionException);
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
