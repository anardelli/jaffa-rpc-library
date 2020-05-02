package com.transport.lib.spring;

import lombok.Getter;

/*
    Class-container for passing list of required server implementations to TransportService bean
 */
@Getter
public class ServerEndpoints {

    // User-provided list of server-side API implementations
    private final Class<?>[] endpoints;

    // Vararg constructor (lombok can't generate it)
    public ServerEndpoints(Class<?>... endpoints) {
        this.endpoints = endpoints;
    }
}
