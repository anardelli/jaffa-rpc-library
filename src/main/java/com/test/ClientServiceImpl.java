package com.test;

import com.transport.lib.zeromq.ApiServer;

@ApiServer
public class ClientServiceImpl implements ClientService {

    @Override
    public void lol3(String message) {
        System.out.println("lol3 " + message);
    }

    @Override
    public void lol4(String message) {
        System.out.println("lol4 " + message);
    }
}
