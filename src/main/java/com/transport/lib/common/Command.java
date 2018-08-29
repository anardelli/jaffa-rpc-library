package com.transport.lib.common;

import com.transport.lib.zookeeper.Utils;

import java.util.Arrays;
import java.util.UUID;

@SuppressWarnings("WeakerAccess, unused")
public class Command {
    private String serviceClass;
    private String methodName;
    private String[] methodArgs;
    private Object[] args;
    private String callbackClass;
    private String callbackKey;
    private String callBackZMQ;
    private String sourceModuleId;
    private String rqUid;
    private SecurityTicket ticket;

    public void setMetadata(){
        try {
            this.callBackZMQ = Utils.getZeroMQCallbackBindAddress();
        }catch (Exception e){
            e.printStackTrace();
        }
        this.sourceModuleId = TransportService.getRequiredOption("module.id");
        this.rqUid = UUID.randomUUID().toString();
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

    public String getCallBackZMQ() {
        return callBackZMQ;
    }

    public String getSourceModuleId() {
        return sourceModuleId;
    }

    public String getRqUid() {
        return rqUid;
    }

    public SecurityTicket getTicket() {
        return ticket;
    }

    public void setTicket(SecurityTicket ticket) {
        this.ticket = ticket;
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
                ", callBackZMQ='" + callBackZMQ + '\'' +
                ", sourceModuleId='" + sourceModuleId + '\'' +
                ", rqUid='" + rqUid + '\'' +
                ", ticket=" + ticket +
                '}';
    }
}