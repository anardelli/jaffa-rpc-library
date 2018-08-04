package com.transport.lib.zeromq;

public interface Callback<T> {
    public void callBack(String key, T result);
    public void callBackError(String key, String stackTrace);
}
