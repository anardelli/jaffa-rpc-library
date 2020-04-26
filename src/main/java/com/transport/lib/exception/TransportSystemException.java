package com.transport.lib.exception;

public class TransportSystemException extends RuntimeException {
    public TransportSystemException(String cause) { super(cause); }
    public TransportSystemException(Throwable cause) { super("Exception occurred during transport call", cause); }
}
