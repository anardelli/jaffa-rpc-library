package com.transport.test;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ServerTest {

    public static void main(String[] args){

        System.out.println("================ TEST SERVER STARTING ================");

        System.setProperty("zookeeper.connection", "localhost:2181");
        System.setProperty("service.root", "com.transport.test");
        System.setProperty("service.port", "4543");
        System.setProperty("module.id", "test.server");
        System.setProperty("use.kafka.for.async", "true");
        System.setProperty("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");

        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.register(MainConfig.class);
        ctx.refresh();

        System.out.println("================ TEST SERVER STARTED ================");
    }
}
