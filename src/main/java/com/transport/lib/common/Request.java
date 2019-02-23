package com.transport.lib.common;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.zookeeper.Utils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;
import scala.collection.Seq;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.transport.lib.common.TransportService.*;
import static java.time.temporal.ChronoUnit.MINUTES;

@SuppressWarnings("all")
public class Request<T> implements RequestInterface<T>{

    private static Logger logger = LoggerFactory.getLogger(Request.class);

    private static final KafkaProducer<String,byte[]> producer = new KafkaProducer<>(producerProps);

    private static final ConcurrentLinkedQueue<KafkaConsumer<String, byte[]>> consumers = new ConcurrentLinkedQueue<>();

    public static void initSyncKafkaConsumers(int brokersCount, CountDownLatch started) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", getRequiredOption("bootstrap.servers"));
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        for(int i = 0; i < brokersCount; i++){
            consumerProps.put("group.id", UUID.randomUUID().toString());
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consumers.add(consumer);
            started.countDown();
        }
    }

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

    private byte[] waitForSyncAnswer(String requestTopic, long requestTime){
        KafkaConsumer<String, byte[]> consumer;

        do{
            consumer = consumers.poll();
        }while (consumer == null);

        String clientTopicName = requestTopic.replace("-server", "-client");
        long tenMinAgo = Instant.ofEpochMilli(requestTime).minus(3, MINUTES).toEpochMilli();
        consumer.subscribe(Collections.singletonList(clientTopicName));
        consumer.poll(0);
        List<PartitionInfo> partitionInfos = consumer.listTopics().get(clientTopicName);
        List<TopicPartition> partitions = new ArrayList<>();
        if (partitionInfos == null) {
            logger.warn("Partition information was not found for topic ", clientTopicName);
        } else {
            for (PartitionInfo partitionInfo : partitionInfos) {
                TopicPartition partition = new TopicPartition(clientTopicName, partitionInfo.partition());
                partitions.add(partition);
            }
        }
        Map<TopicPartition, Long> query = new HashMap<>();
        partitions.forEach(x -> query.put(x, tenMinAgo));
        Map<TopicPartition, OffsetAndTimestamp> result = consumer.offsetsForTimes(query);
        for(Map.Entry<TopicPartition, OffsetAndTimestamp> entry: result.entrySet()){
            if(entry.getValue() == null) continue;
            consumer.seek(entry.getKey(), entry.getValue().offset());
        }

        long start = System.currentTimeMillis();
        while(true){
            if(timeout != -1 && System.currentTimeMillis() - start > timeout) break;
            ConsumerRecords<String, byte[]> records = consumer.poll(10);
            for(ConsumerRecord<String,byte[]> record: records){
                if(record.key().equals(command.getRqUid())){
                    try {
                        Map<TopicPartition, OffsetAndMetadata> commitData = new HashMap<>();
                        commitData.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset()));
                        consumer.commitSync(commitData);
                    }catch (CommitFailedException e){
                        logger.error("Error during commit received answer", e);
                    }
                    consumers.add(consumer);
                    return record.value();
                }
            }
        }
        consumers.add(consumer);
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
                logger.error("Error in sending sync request", e);
            }
            response = waitForSyncAnswer(requestTopic, System.currentTimeMillis());
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
                logger.error("Error in sending async request", e);
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
