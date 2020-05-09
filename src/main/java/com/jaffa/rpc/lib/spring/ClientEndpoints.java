package com.jaffa.rpc.lib.spring;

import lombok.Getter;

@Getter
public class ClientEndpoints {
    private final Class<?>[] endpoints;

    public ClientEndpoints(Class<?>... endpoints) {
        this.endpoints = endpoints;
    }
}
