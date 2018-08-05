package com.transport.lib.zeromq;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.transport.lib.zookeeper.ZKUtils;
import kafka.zk.KafkaZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Time;
import org.zeromq.ZMQ;
import scala.collection.Seq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.transport.lib.zeromq.ZeroRPCService.getOption;

public class Request<T> implements RequestInterface<T>{

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

    @SuppressWarnings("unchecked")
    public T executeSync(){
        ZMQ.Context context =  ZMQ.context(1);
        ZMQ.Socket socket = context.socket(ZMQ.REQ);
        socket.connect("tcp://" + ZKUtils.getHostForService(command.getServiceClass(), moduleId));
        Kryo kryo = new Kryo();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        socket.send(out.toByteArray(), 0);
        if(timeout != -1) {
            socket.setReceiveTimeOut(timeout);
        }
        byte[] response = socket.recv(0);
        ZKUtils.closeSocketAndContext(socket,context);
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

    public static String getTopicForService(String service, String moduleId){
        String serviceInterface = service.replace("Transport", "");
        KafkaZkClient zkClient = KafkaZkClient.apply(getOption("zookeeper.connection"),false,200000, 15000,10,Time.SYSTEM,UUID.randomUUID().toString(),UUID.randomUUID().toString());
        if(moduleId != null){
            String topicName = serviceInterface + "-" + moduleId + "-server";
            if(!zkClient.topicExists(topicName))
                throw new RuntimeException("No route for service: " + serviceInterface);
            else
                return topicName;
        }else {
            Seq<String> allTopic = zkClient.getAllTopicsInCluster();
            List<String> topics = scala.collection.JavaConversions.seqAsJavaList(allTopic);
            List<String> filtered = topics.stream().filter(x -> x.startsWith(serviceInterface+"-")).filter(x -> x.endsWith("-server")).collect(Collectors.toList());
            if(filtered.isEmpty()) throw new RuntimeException("No route for service: " + serviceInterface);
            else
                return filtered.get(0);
        }
    }

    public void executeAsync(String key, Class listener){
        command.setCallbackClass(listener.getName());
        command.setCallbackKey(key);
        Kryo kryo = new Kryo();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Output output = new Output(out);
        kryo.writeObject(output, command);
        output.close();
        if(ZKUtils.useKafkaForAsync()){
            Properties producerProps = new Properties();
            producerProps.put("bootstrap.servers", getOption("bootstrap.servers"));
            producerProps.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
            producerProps.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
            KafkaProducer<String,byte[]> producer = new KafkaProducer<>(producerProps);
            try{
                ProducerRecord<String,byte[]> resultPackage = new ProducerRecord<>(getTopicForService(command.getServiceClass(), moduleId), UUID.randomUUID().toString(), out.toByteArray());
                producer.send(resultPackage).get();
            }catch (Exception e){
                e.printStackTrace();
            }
        }else {
            ZMQ.Context context =  ZMQ.context(1);
            ZMQ.Socket socket = context.socket(ZMQ.REQ);
            socket.connect("tcp://" + ZKUtils.getHostForService(command.getServiceClass(), moduleId));
            socket.send(out.toByteArray(), 0);
            socket.recv(0);
            ZKUtils.closeSocketAndContext(socket,context);
        }
    }
}
