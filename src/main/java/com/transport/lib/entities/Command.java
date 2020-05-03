package com.transport.lib.entities;

import com.transport.lib.security.SecurityTicket;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/*
    Class-container for passing all required information about remote method invocation
 */
@NoArgsConstructor
@Setter
@Getter
@ToString
public class Command {

    // Fully-qualified target class name
    private String serviceClass;
    // Target method name
    private String methodName;
    // List of filly-qualified class names of method's arguments
    private String[] methodArgs;
    // List of arguments for invocation
    private Object[] args;
    // For async calls: fully-qualified callback class name
    private String callbackClass;
    // For async calls: user-provided unique key for original request identification
    private String callbackKey;
    // For async calls: zmq callback receiver listener address like host:port
    private String callBackZMQ;
    // Client's module.id
    private String sourceModuleId;
    // Unique ID (UUID), used for internal purposes
    private String rqUid;
    // SecurityTicket associated with this invocation
    private SecurityTicket ticket;
    // For async calls: moment in the future after which Callback will receive "Transport execution timeout"
    private long asyncExpireTime;
    private long requestTime;
    private long localRequestTime;
}