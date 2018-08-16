package com.transport.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class ServerTest {

    private static Logger logger = LoggerFactory.getLogger(ServerTest.class);

    public static void main(String[] args){

        logger.info("================ TEST SERVER STARTING ================");

        System.setProperty("zookeeper.connection", "localhost:2181");
        System.setProperty("service.root", "com.transport.test");
        System.setProperty("service.port", "4543");
        System.setProperty("module.id", "test.server");
        System.setProperty("use.kafka.for.async", "true");
        System.setProperty("use.kafka.for.sync", "true");
        System.setProperty("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");

        AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.register(MainConfig.class);
        ctx.refresh();

        logger.info("================ TEST SERVER STARTED ================");
    }
}
