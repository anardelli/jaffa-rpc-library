package com.transport.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import java.io.IOException;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes={MainConfig.class}, loader=AnnotationConfigContextLoader.class)
public class MainTest {

    @Autowired
    PersonServiceTransport personService;

    @Autowired
    ClientServiceTransport clientServiceTransport;

    @Test
    public void testMethods() {

        Integer id = personService.add("James Carr", "james@zapier.com", null).withTimeout(10_000).onModule("test.server").execute();
        System.out.printf("Resulting id is %s", id);
        System.out.println();
        Person person = personService.get(id).onModule("test.server").execute();
        System.out.println(person);
        personService.lol().execute();
        personService.lol2("kek").execute();
        System.out.println("Name: "  + personService.getName().execute());

        clientServiceTransport.lol3("test3").onModule("main.server").execute();
        clientServiceTransport.lol4("test4").onModule("main.server").execute();
    }
}
