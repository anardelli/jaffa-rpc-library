package com.transport.test;

import com.transport.lib.annotations.ApiServer;
import com.transport.lib.entities.RequestContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@ApiServer
public class PersonServiceImpl implements PersonService {
    
    private final List<Person> people = new ArrayList<>();

    private final AtomicInteger idProvider = new AtomicInteger(1);

    public int add(String name, String email, Address address) {
        log.info("SOURCE MODULE ID: " + RequestContext.getSourceModuleId() + " MY MODULE ID: " + System.getProperty("module.id"));
        log.info("TICKET: " + RequestContext.getTicket());
        Person p = new Person();
        p.setEmail(email);
        p.setName(name);
        p.setId(idProvider.addAndGet(1));
        p.setAddress(address);
        people.add(p);
        return p.getId();
    }

    public Person get(final Integer id) {
        log.info("SOURCE MODULE ID: " + RequestContext.getSourceModuleId() + " MY MODULE ID: " + System.getProperty("module.id"));
        log.info("TICKET: " + RequestContext.getTicket());
        for (Person p : this.people) {
            if (p.getId().equals(id)) {
                return p;
            }
        }
        return null;
    }

    public void lol() {
        log.info("SOURCE MODULE ID: " + RequestContext.getSourceModuleId() + " MY MODULE ID: " + System.getProperty("module.id"));
        log.info("TICKET: " + RequestContext.getTicket());
        log.info("Lol");
    }

    public void lol2(String message) {
        log.info(message);
    }

    public String getName() {
        return null;
    }

    public Person testError() {
        throw new RuntimeException("very bad in " + System.getProperty("module.id"));
    }
}