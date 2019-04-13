package com.transport.test;

import com.transport.lib.common.Api;

@Api
public interface PersonService {

    public static final String TEST = "TEST";

    public static void lol3() {
        System.out.println("lol3");
    }

    public int add(String name, String email, Address address);

    public Person get(Integer id);

    public void lol();

    public void lol2(String message);

    public String getName();

    public String testError();

}
