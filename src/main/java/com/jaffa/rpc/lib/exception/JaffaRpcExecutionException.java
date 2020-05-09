package com.jaffa.rpc.lib.exception;

public class JaffaRpcExecutionException extends RuntimeException {
    public JaffaRpcExecutionException(String cause) {
        super(cause);
    }

    public JaffaRpcExecutionException(Throwable cause) {
        super("Exception occurred during RPC call", cause);
    }
}