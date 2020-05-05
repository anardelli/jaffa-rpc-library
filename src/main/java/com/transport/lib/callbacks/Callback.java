package com.transport.lib.callbacks;

public interface Callback<T> {

    void onSuccess(String key, T result);

    void onError(String key, Throwable exception);
}
