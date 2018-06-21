package com.test;

import com.transport.lib.zeromq.ApiServer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@ApiServer
public class PersonServiceImpl implements PersonService{
    private List<Person> people = new ArrayList<Person>();
    private AtomicInteger idProvider = new AtomicInteger(1);

    public int add(String name, String email, Address address) {
        Person p = new Person();
        p.setEmail(email);
        p.setName(name);
        p.setId(idProvider.addAndGet(1));
        p.setAddress(address);
        people.add(p);
        return p.getId();
    }

    public Person get(final Integer id) {
        for (Person p : this.people) {
            if (p.getId() == id) {
                return p;
            }
        }
        return null;
    }

    public void lol(){
        System.out.println("Lol");
    }

    public void lol2(String message){
        System.out.println(message);
    }

    public String getName(){
        return null;
    }
}