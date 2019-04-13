package com.transport.lib.common;

@SuppressWarnings("all")
public class ClientEndpoints {
    private Class[] clientEndpoints = null;

    public ClientEndpoints(Class... endpoints) {
        this.clientEndpoints = endpoints;
    }

    public Class[] getClientEndpoints() {
        return clientEndpoints;
    }
}
