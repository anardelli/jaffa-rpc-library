package com.transport.test;

import com.transport.lib.zeromq.Callback;

public class ServiceCallback implements Callback<Void> {

    @Override
    public void callBack(String key, Void result) {
        System.out.println("Key: " + key);
        System.out.println("Result: " + result);
    }

    @Override
    public void callBackError(String key, Throwable exception) {
        System.out.println("Exception during async call");
        exception.printStackTrace();
    }
}
