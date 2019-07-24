package com.transport.lib.common;

import lombok.Getter;

/*
    Class-container for passing list of required server implemenations to TransportService bean
 */
@SuppressWarnings("all")
@Getter
public class ServerEndpoints {

    // User-provided list of server-side API implementations
    private Class[] serverEndpoints = null;

    // Vararg constructor (lombok can't generate it)
    public ServerEndpoints(Class... endpoints) {
        this.serverEndpoints = endpoints;
    }
}
