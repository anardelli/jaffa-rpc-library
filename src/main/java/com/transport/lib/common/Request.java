package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.zookeeper.Utils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.zeromq.ZMQ;
import scala.collection.Seq;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;
import java.util.stream.Collectors;

import static com.transport.lib.common.TransportService.*;

public class Request<T> implements RequestInterface<T>{

    private static final KafkaProducer<String,byte[]> producer = new KafkaProducer<>(producerProps);

    private int timeout = -1;
    private String moduleId;
    private Command command;

    public Request(Command command){
        this.command = command;
    }

    public Request<T> withTimeout(int timeout){
        this.timeout = timeout;
        return this;
    }

    public Request<T> onModule(String moduleId){
        this.moduleId = moduleId;
        return this;
    }

    private byte[] waitForSyncAnswer(String requestTopic){
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("group.id", UUID.randomUUID().toString());
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(requestTopic.replace("-server", "-client")));
        long elapsed = 0;
        long start = System.currentTimeMillis();
        while(timeout == -1 || elapsed < timeout){
            elapsed += (System.currentTimeMillis() - start);
            ConsumerRecords<String, byte[]> records = consumer.poll(10);
            for(ConsumerRecord<String,byte[]> record: records){
                if(record.key().equals(command.getRqUid())){
                    try {
                        Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                        commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        consumer.commitSync(commitData);
                    }catch (CommitFailedException e){
                        e.printStackTrace();
                    }
                    return record.value();
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public T executeSync(){
        Kryo kryo = new Kryo();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        byte[] response;
        if(Utils.useKafkaForSync()){
            String requestTopic = getTopicForService(command.getServiceClass(), moduleId, true);
            try{
                ProducerRecord<String,byte[]> resultPackage = new ProducerRecord<>(requestTopic, UUID.randomUUID().toString(), out.toByteArray());
                producer.send(resultPackage).get();
            }catch (Exception e){
                e.printStackTrace();
            }
            response = waitForSyncAnswer(requestTopic);
        }else {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket socket = context.socket(ZMQ.REQ);
            socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId));
            socket.send(out.toByteArray(), 0);
            if (timeout != -1) {
                socket.setReceiveTimeOut(timeout);
            }
            response = socket.recv(0);
            Utils.closeSocketAndContext(socket, context);
        }
        if(response == null) {
            throw new RuntimeException("Transport execution timeout");
        }
        Input input = new Input(new ByteArrayInputStream(response));
        Object result = kryo.readClassAndObject(input);
        input.close();
        if(result instanceof ExceptionHolder)
            throw new RuntimeException(((ExceptionHolder) result).getStackTrace());
        return (T)result;
    }

    private static String getTopicForService(String service, String moduleId, boolean sync){
        String serviceInterface = service.replace("Transport", "");
        String type = sync ? "-sync" : "-async";
        if(moduleId != null){
            String topicName = serviceInterface + "-" + moduleId + "-server" + type;
            if(!zkClient.topicExists(topicName))
                throw new RuntimeException("No route for service: " + serviceInterface);
            else
                return topicName;
        }else {
            Seq<String> allTopic = zkClient.getAllTopicsInCluster();
            List<String> topics = scala.collection.JavaConversions.seqAsJavaList(allTopic);
            List<String> filtered = topics.stream().filter(x -> x.startsWith(serviceInterface+"-")).filter(x -> x.endsWith("-server" + type)).collect(Collectors.toList());
            if(filtered.isEmpty()) throw new RuntimeException("No route for service: " + serviceInterface);
            else
                return filtered.get(0);
        }
    }

    public void executeAsync(String key, Class listener){
        command.setCallbackClass(listener.getName());
        command.setCallbackKey(key);
        Kryo kryo = new Kryo();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        if(Utils.useKafkaForAsync()){
            try{
                ProducerRecord<String,byte[]> resultPackage = new ProducerRecord<>(getTopicForService(command.getServiceClass(), moduleId, false), UUID.randomUUID().toString(), out.toByteArray());
                producer.send(resultPackage).get();
            }catch (Exception e){
                e.printStackTrace();
            }
        }else {
            ZMQ.Context context =  ZMQ.context(1);
            ZMQ.Socket socket = context.socket(ZMQ.REQ);
            socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId));
            socket.send(out.toByteArray(), 0);
            socket.recv(0);
            Utils.closeSocketAndContext(socket,context);
        }
    }
}
