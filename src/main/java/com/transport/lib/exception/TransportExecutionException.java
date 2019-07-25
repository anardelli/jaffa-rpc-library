package com.transport.lib.exception;

public class TransportExecutionException extends RuntimeException {
    public TransportExecutionException(String cause) { super(cause); }
    public TransportExecutionException(Throwable cause) { super("Exception occurred during transport call", cause); }
}