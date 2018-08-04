package com.transport.lib.zeromq;

import java.util.Arrays;

@SuppressWarnings("WeakerAccess, unused")
public class Command {
    private String serviceClass;
    private String methodName;
    private String[] methodArgs;
    private Object[] args;
    private String callbackClass;
    private String callbackKey;

    public Command() {}

    public Command(String serviceClass, String methodName, String[] methodArgs, Object... args) {
        this.serviceClass = serviceClass;
        this.methodArgs = methodArgs;
        this.methodName = methodName;
        this.args = args;
    }

    public String getServiceClass() {
        return serviceClass;
    }

    public void setServiceClass(String serviceClass) {
        this.serviceClass = serviceClass;
    }

    public String getMethodName() {
        return methodName;
    }

    public void setMethodName(String methodName) {
        this.methodName = methodName;
    }

    public String[] getMethodArgs() {
        return methodArgs;
    }

    public void setMethodArgs(String[] methodArgs) {
        this.methodArgs = methodArgs;
    }

    public Object[] getArgs() {
        return args;
    }

    public void setArgs(Object[] args) {
        this.args = args;
    }

    public String getCallbackClass() {
        return callbackClass;
    }

    public void setCallbackClass(String callbackClass) {
        this.callbackClass = callbackClass;
    }

    public String getCallbackKey() {
        return callbackKey;
    }

    public void setCallbackKey(String callbackKey) {
        this.callbackKey = callbackKey;
    }

    @Override
    public String toString() {
        return "Command{" +
                "serviceClass='" + serviceClass + '\'' +
                ", methodName='" + methodName + '\'' +
                ", methodArgs=" + Arrays.toString(methodArgs) +
                ", args=" + Arrays.toString(args) +
                ", callbackClass='" + callbackClass + '\'' +
                ", callbackKey='" + callbackKey + '\'' +
                '}';
    }
}