package com.transport.lib.zeromq;

public class ExceptionHolder {

    public ExceptionHolder(){
    }

    public ExceptionHolder(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    public String getStackTrace() {
        return stackTrace;
    }

    public void setStackTrace(String stackTrace) {
        this.stackTrace = stackTrace;
    }

    private String stackTrace;

}
