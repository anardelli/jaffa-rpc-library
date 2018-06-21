package com.transport.lib.zeromq;

public class ExceptionHolder {

    public ExceptionHolder(){
    }

    ExceptionHolder(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    private String stackTrace;

}
