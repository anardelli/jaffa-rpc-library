package com.transport.test;

import com.transport.lib.zeromq.Api;

@Api
public interface PersonService {

    public static final String FUCK = "FUCK";
    public int add(String name,  String email, Address address);
    public Person get(Integer id);
    public void lol();
    public void lol2(String message);
    public static void shit(){
        System.out.println("Shit");
    }
    public String getName();

}
