package com.transport.lib.exception;

public class TransportSystemException extends RuntimeException {
    public static final String NO_PROTOCOL_DEFINED = "No known protocol defined";

    public TransportSystemException(String cause) {
        super(cause);
    }

    public TransportSystemException(Throwable cause) {
        super("Exception occurred during transport call", cause);
    }
}
