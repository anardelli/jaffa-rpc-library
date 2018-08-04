package com.transport.lib.zeromq;

@SuppressWarnings("unused")
public class ExceptionHolder {

    private String stackTrace;

    public ExceptionHolder(){ }

    ExceptionHolder(String stackTrace) { this.stackTrace = stackTrace; }

    String getStackTrace() { return stackTrace; }
}
