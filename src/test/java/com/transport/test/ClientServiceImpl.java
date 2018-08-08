package com.transport.test;

import com.transport.lib.common.ApiServer;

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
