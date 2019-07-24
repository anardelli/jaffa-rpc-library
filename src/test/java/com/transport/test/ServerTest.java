package com.transport.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.UUID;

public class ServerTest {

    private static Logger logger = LoggerFactory.getLogger(ServerTest.class);

    public static void main(String[] args) {

        logger.info("================ TEST SERVER STARTING ================");

        System.setProperty("zookeeper.connection", "localhost:2181");
        System.setProperty("service.port", "4543");
        System.setProperty("module.id", "test.server");
        System.setProperty("use.kafka", "true");
        System.setProperty("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");

        final AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
        ctx.register(MainConfig.class);
        ctx.refresh();

        Runtime.getRuntime().addShutdownHook(new Thread(ctx::close));

        PersonServiceTransport personService = ctx.getBean(PersonServiceTransport.class);
        ClientServiceTransport clientService = ctx.getBean(ClientServiceTransport.class);

        try {
            Thread.sleep(5_000);
        } catch (Exception ignore) {
        }
        Integer id = personService.add("Test name", "test@mail.com", null).withTimeout(15_000).onModule("test.server").executeSync();
        logger.info("Resulting id is " + id);
        Person person = personService.get(id).onModule("test.server").executeSync();
        logger.info(person.toString());
        personService.lol().executeSync();
        personService.lol2("kek").executeSync();
        logger.info("Name: " + personService.getName().executeSync());
        clientService.lol3("test3").onModule("test.server").executeSync();
        clientService.lol4("test4").onModule("test.server").executeSync();
        clientService.lol4("test4").onModule("test.server").withTimeout(10_000).executeAsync(UUID.randomUUID().toString(), ServiceCallback.class);
        personService.get(id).onModule("test.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);
        personService.lol2("kek").executeSync();
        try {
            personService.testError().onModule("test.server").executeSync();
        } catch (Throwable e) {
            logger.error("Exception during sync call:", e);
        }
        personService.testError().onModule("test.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);

        logger.info("================ TEST SERVER STARTED ================");
    }
}
