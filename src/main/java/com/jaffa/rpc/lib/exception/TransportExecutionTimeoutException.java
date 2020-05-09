package com.jaffa.rpc.lib.exception;

public class TransportExecutionTimeoutException extends RuntimeException {
    public TransportExecutionTimeoutException() {
        super("Transport execution timeout");
    }
}
