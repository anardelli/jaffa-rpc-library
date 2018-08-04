package com.transport.lib.zeromq;

@SuppressWarnings("WeakerAccess, unused")
public class CallbackContainer {
    private String key;
    private String listener;
    private Object result;
    private String resultClass;

    public CallbackContainer(){}

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getListener() {
        return listener;
    }

    public void setListener(String listener) {
        this.listener = listener;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    public String getResultClass() {
        return resultClass;
    }

    public void setResultClass(String resultClass) {
        this.resultClass = resultClass;
    }

    @Override
    public String toString() {
        return "CallbackContainer{" +
                "key='" + key + '\'' +
                ", listener='" + listener + '\'' +
                ", result=" + result +
                ", resultClass='" + resultClass + '\'' +
                '}';
    }
}
