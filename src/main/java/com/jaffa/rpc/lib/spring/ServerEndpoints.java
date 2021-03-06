package com.jaffa.rpc.lib.spring;

import lombok.Getter;

@Getter
public class ServerEndpoints {
    private final Class<?>[] endpoints;

    public ServerEndpoints(Class<?>... endpoints) {
        this.endpoints = endpoints;
    }
}
