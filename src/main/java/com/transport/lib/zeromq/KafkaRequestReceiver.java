package com.transport.lib.zeromq;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import kafka.admin.RackAwareMode;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.reflections.Reflections;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Method;
import java.util.*;
import static com.transport.lib.zeromq.ZeroRPCService.*;

@SuppressWarnings("WeakerAccess, unchecked")
public class KafkaRequestReceiver implements Runnable {

    public static volatile boolean active = true;
    private static final HashSet<String> serverTopics = new HashSet<>();
    private static final ArrayList<Thread> serverConsumers = new ArrayList<>(3);

    @Override
    public void run() {
        new Reflections(getOption("service.root")).getTypesAnnotatedWith(Api.class).forEach(x -> {if(x.isInterface()) serverTopics.add(x.getName() + "-" + getOption("module.id") + "-server");});
        Properties topicConfig = new Properties();
        serverTopics.forEach(topic -> {
            if(!zkClient.topicExists(topic)){
                adminZkClient.createTopic(topic,3,1,topicConfig,RackAwareMode.Disabled$.MODULE$);
            }
        });
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", getOption("bootstrap.servers"));
        consumerProps.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer","org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("auto.commit.offset","false");
        consumerProps.put("group.id", UUID.randomUUID().toString());

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", getOption("bootstrap.servers"));
        producerProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");

        Runnable consumerThread = () ->  {
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            KafkaProducer<String,byte[]> producer = new KafkaProducer<>(producerProps);
            consumer.subscribe(serverTopics);
            while(active){
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for(ConsumerRecord<String,byte[]> record: records){
                    try {
                        System.out.println(Thread.currentThread().getName());
                        Kryo kryo = new Kryo();
                        Input input = new Input(new ByteArrayInputStream(record.value()));
                        final Command command = kryo.readObject(input, Command.class);
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
                        ProducerRecord<String,byte[]> resultPackage = new ProducerRecord<>(command.getServiceClass().replace("Transport", "") + "-" + command.getSourceModuleId() + "-client", UUID.randomUUID().toString(), bOutput.toByteArray());
                        producer.send(resultPackage).get();
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                }
                try {
                    consumer.commitSync();
                }catch (CommitFailedException e){
                    e.printStackTrace();
                }
            }
        };
        serverConsumers.add(new Thread(consumerThread));
        serverConsumers.add(new Thread(consumerThread));
        serverConsumers.add(new Thread(consumerThread));
        serverConsumers.forEach(Thread::start);
        serverConsumers.forEach(x -> {try{x.join();} catch (Exception e){e.printStackTrace();}});
    }
}
