package com.transport.lib.zeromq;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import kafka.admin.RackAwareMode;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.reflections.Reflections;
import java.io.ByteArrayInputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import static com.transport.lib.zeromq.ZeroRPCService.*;

@SuppressWarnings("WeakerAccess, unchecked")
public class KafkaAsyncResponseReceiver implements Runnable {
    public static volatile boolean active = true;
    private static final HashSet<String> clientTopics = new HashSet<>();
    private static final ArrayList<Thread> clientConsumers = new ArrayList<>(3);

    @Override
    public void run() {
        new Reflections(getOption("service.root")).getTypesAnnotatedWith(Api.class).forEach(x -> {if(x.isInterface()) clientTopics.add(x.getName() + "-" + getOption("module.id") + "-client-async");});
        Properties topicConfig = new Properties();
        clientTopics.forEach(topic -> {
            if(!zkClient.topicExists(topic)){
                adminZkClient.createTopic(topic,3,1,topicConfig,RackAwareMode.Disabled$.MODULE$);
            }
        });
        Runnable consumerThread = () ->  {
            KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(clientTopics);
            while(active){
                ConsumerRecords<String, byte[]> records = consumer.poll(100);
                for(ConsumerRecord<String,byte[]> record: records){
                    try {
                        Kryo kryo = new Kryo();
                        Input input = new Input(new ByteArrayInputStream(record.value()));
                        final CallbackContainer callbackContainer = kryo.readObject(input, CallbackContainer.class);
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
                    } catch (Exception e) {
                        System.out.println("Error during receiving callback:");
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
        clientConsumers.add(new Thread(consumerThread));
        clientConsumers.add(new Thread(consumerThread));
        clientConsumers.add(new Thread(consumerThread));
        clientConsumers.forEach(Thread::start);
        clientConsumers.forEach(x -> {try{x.join();} catch (Exception e){e.printStackTrace();}});
    }
}
