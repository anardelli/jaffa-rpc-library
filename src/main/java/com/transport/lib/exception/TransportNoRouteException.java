package com.transport.lib.exception;

import com.transport.lib.common.Protocol;

public class TransportNoRouteException extends RuntimeException {
    public TransportNoRouteException(String service, String moduleId){
        super("No route for service: " + service + (moduleId  != null ? (" and module.id " + moduleId) : ""));
    }
    public TransportNoRouteException(String service){
        super("No route for service: " + service);
    }
    public TransportNoRouteException(String service, Protocol protocol){
        super("No route for service: " + service + " and protocol " + protocol.getShortName());
    }
}
