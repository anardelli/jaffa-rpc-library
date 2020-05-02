package com.transport.lib.exception;

import com.transport.lib.entities.Protocol;

public class TransportNoRouteException extends RuntimeException {
    private static final String MESSAGE_PREFIX = "No route for service: ";
    public TransportNoRouteException(String service, String moduleId) {
        super(MESSAGE_PREFIX + service + (moduleId != null ? (" and module.id " + moduleId) : ""));
    }

    public TransportNoRouteException(String service) {
        super(MESSAGE_PREFIX + service);
    }

    public TransportNoRouteException(String service, Protocol protocol) {
        super(MESSAGE_PREFIX + service + " and protocol " + protocol.getShortName());
    }
}
