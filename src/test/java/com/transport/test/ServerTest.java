package com.transport.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {MainConfig.class}, loader = AnnotationConfigContextLoader.class)
public class ServerTest {

    @Autowired
    private PersonServiceTransport personService;

    @Autowired
    private ClientServiceTransport clientService;

    @BeforeClass
    public static void before() {
        log.info("================ TEST SERVER STARTING ================");
        System.setProperty("zookeeper.connection", "localhost:2181");
        System.setProperty("http.service.port", "4543");
        System.setProperty("http.callback.port", "4343");
        System.setProperty("zmq.service.port", "4843");
        System.setProperty("zmq.callback.port", "4943");
        System.setProperty("module.id", "test.server");
        System.setProperty("transport.protocol", "http");
        System.setProperty("bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093");
    }

    @Test
    @Ignore
    public void stage1() {
        Integer id = personService.add("Test name", "test@mail.com", null).withTimeout(TimeUnit.MILLISECONDS.toMillis(15000)).onModule("test.server").executeSync();
        log.info("Resulting id is {}", id);
        Person person = personService.get(id).onModule("test.server").executeSync();
        Assert.assertEquals(person.getId(), id);
        log.info(person.toString());
        personService.lol().executeSync();
        personService.lol2("kek").executeSync();
        String name = personService.getName().executeSync();
        log.info("Name: {}", name);
        Assert.assertNull(name);
        clientService.lol3("test3").onModule("test.server").executeSync();
        clientService.lol4("test4").onModule("test.server").executeSync();
        clientService.lol4("test4").onModule("test.server").withTimeout(TimeUnit.MILLISECONDS.toMillis(10000)).executeAsync(UUID.randomUUID().toString(), ServiceCallback.class);
        personService.get(id).onModule("test.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);
        personService.lol2("kek").executeSync();
        try {
            personService.testError().onModule("test.server").executeSync();
        } catch (Throwable e) {
            log.error("Exception during sync call:", e);
            Assert.assertTrue(e.getMessage().contains("very bad in"));
        }
        personService.testError().onModule("test.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);
    }

    @Test
    public void stage2() {
        // 1 hour load test
        final boolean sync = true;
        final boolean heavy = true;
        Runnable runnable = () -> {
            long startTime = System.currentTimeMillis();
            while (!Thread.currentThread().isInterrupted() && (System.currentTimeMillis() - startTime) < (60 * 60 * 1000)) {
                if (sync) {
                    if(heavy){
                        personService.getHeavy(RandomStringUtils.randomAlphabetic(250_000)).onModule("test.server").executeSync();
                    }else {
                        // Sync call
                        clientService.lol3("test3").onModule("test.server").executeSync();
                    }
                } else {
                    // Async call
                    clientService.lol3("test3").onModule("test.server").withTimeout(10_000).executeAsync(UUID.randomUUID().toString(), ServiceCallback.class);
                }
                try {
                    Thread.sleep((int) (Math.random() * 100));
                } catch (InterruptedException exception) {
                    exception.printStackTrace();
                }
            }
        };
        try {
            Thread thread = new Thread(runnable);
            thread.start();
            thread.join();
        } catch (Exception ignore) {
        }
    }

    @AfterClass
    public static void after() {
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (Exception ignore) {
        }
    }
}
