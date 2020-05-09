package com.jaffa.rpc.lib.exception;

public class JaffaRpcExecutionTimeoutException extends RuntimeException {
    public JaffaRpcExecutionTimeoutException() {
        super("RPC execution timeout");
    }
}
