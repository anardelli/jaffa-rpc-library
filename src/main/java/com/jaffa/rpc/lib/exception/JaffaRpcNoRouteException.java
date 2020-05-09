package com.jaffa.rpc.lib.exception;

import com.jaffa.rpc.lib.entities.Protocol;

public class JaffaRpcNoRouteException extends RuntimeException {
    private static final String MESSAGE_PREFIX = "No route for service: ";

    public JaffaRpcNoRouteException(String service, String moduleId) {
        super(MESSAGE_PREFIX + service + (moduleId != null ? (" and module.id " + moduleId) : ""));
    }

    public JaffaRpcNoRouteException(String service) {
        super(MESSAGE_PREFIX + service);
    }

    public JaffaRpcNoRouteException(String service, Protocol protocol) {
        super(MESSAGE_PREFIX + service + " and protocol " + protocol.getShortName());
    }
}
