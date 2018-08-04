package com.transport.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.io.IOException;
import java.util.UUID;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={MainConfig.class}, loader=AnnotationConfigContextLoader.class)
public class MainTest {

    @Autowired
    private PersonServiceTransport personService;

    @Autowired
    private ClientServiceTransport clientService;

    @Test
    public void testMethods() {

        Integer id = personService.add("James Carr", "james@zapier.com", null).withTimeout(10_000).onModule("main.server").executeSync();
        System.out.printf("Resulting id is %s", id);
        System.out.println();
        Person person = personService.get(id).onModule("main.server").executeSync();
        System.out.println(person);
        personService.lol().executeSync();
        personService.lol2("kek").executeSync();
        System.out.println("Name: "  + personService.getName().executeSync());
        clientService.lol3("test3").onModule("main.server").executeSync();
        clientService.lol4("test4").onModule("main.server").executeSync();
        clientService.lol4("test4").onModule("main.server").executeAsync(UUID.randomUUID().toString(), ServiceCallback.class);
        personService.get(id).onModule("main.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);
        personService.lol2("kek").executeSync();
        try {
            personService.testError().onModule("main.server").executeSync();
        }catch (Exception e){
            System.out.println("Exception during sync call");
            e.printStackTrace();
        }
        try {
            personService.testError().onModule("main.server").executeAsync(UUID.randomUUID().toString(), PersonCallback.class);
        }catch (Exception e){
            System.out.println("Exception during async call");
            e.printStackTrace();
        }
    }
}
