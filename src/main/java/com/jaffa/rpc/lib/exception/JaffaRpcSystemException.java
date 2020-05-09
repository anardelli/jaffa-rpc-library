package com.jaffa.rpc.lib.exception;

public class JaffaRpcSystemException extends RuntimeException {
    public static final String NO_PROTOCOL_DEFINED = "No known protocol defined";

    public JaffaRpcSystemException(String cause) {
        super(cause);
    }

    public JaffaRpcSystemException(Throwable cause) {
        super("Exception occurred during RPC call", cause);
    }
}
