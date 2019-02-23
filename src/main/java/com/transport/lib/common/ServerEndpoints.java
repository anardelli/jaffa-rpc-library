package com.transport.lib.common;

@SuppressWarnings("all")
public class ServerEndpoints {
    private Class[] serverEndpoints = null;
    public ServerEndpoints(Class... endpoints){ this.serverEndpoints = endpoints; }
    public Class[] getServerEndpoints() { return serverEndpoints; }
}
