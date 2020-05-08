package com.transport.test;

import com.transport.lib.exception.TransportExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.util.UUID;

@Slf4j
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {MainConfig.class}, loader = AnnotationConfigContextLoader.class)
public class MainTest {

    @Autowired
    private PersonServiceClient personService;

    @Autowired
    private ClientServiceClient clientService;

    @Test
    public void testMethods() {
        Runnable runnable = () -> {
            Integer id = personService.add("Test name 2", "test2@mail.com", null).withTimeout(15_000).onModule("main.server").executeSync();
            log.info("Resulting id is " + id);
            Person person = personService.get(id).onModule("main.server").executeSync();
            log.info(person.toString());
            personService.lol().executeSync();
            personService.lol2("kek").executeSync();
            log.info("Name: " + personService.getName().executeSync());
            clientService.lol3("test3").onModule("main.server").executeSync();
            clientService.lol4("test4").onModule("main.server").executeSync();
            clientService.lol4("test4").onModule("main.server").executeAsync(UUID.randomUUID().toString(), ServiceCallback.class);
            personService.get(id).onModule("main.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);
            personService.lol2("kek").executeSync();
            try {
                personService.testError().onModule("main.server").executeSync();
            } catch (Exception e) {
                log.error("Exception during sync call:", e);
            }
            personService.testError().onModule("main.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);

            id = personService.add("Test name 2", "test2@mail.com", null).withTimeout(10_000).onModule("test.server").executeSync();
            log.info("Resulting id is " + id);
            person = personService.get(id).onModule("test.server").executeSync();
            log.info(person.toString());
            personService.lol().executeSync();
            personService.lol2("kek").executeSync();
            log.info("Name: " + personService.getName().executeSync());
            clientService.lol3("test3").onModule("test.server").executeSync();
            clientService.lol4("test4").onModule("test.server").executeSync();
            clientService.lol4("test4").onModule("test.server").withTimeout(10_000).executeAsync(UUID.randomUUID().toString(), ServiceCallback.class);
            personService.get(id).onModule("test.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);
            personService.lol2("kek").executeSync();
            try {
                personService.testError().onModule("test.server").executeSync();
            } catch (TransportExecutionException e) {
                log.error("Exception during sync call:", e);
            }
            personService.testError().onModule("test.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);
        };

        Thread thread1 = new Thread(runnable);
        Thread thread2 = new Thread(runnable);
        Thread thread3 = new Thread(runnable);

        thread1.start();
        thread2.start();
        thread3.start();

        try {
            thread1.join();
            thread2.join();
            thread3.join();
            Thread.sleep(20_000);
        } catch (Exception ignore) {
        }
    }
}
