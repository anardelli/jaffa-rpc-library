package com.transport.lib.common;

public interface Callback<T> {
    public void callBack(String key, T result);
    public void callBackError(String key, Throwable exception);
}
