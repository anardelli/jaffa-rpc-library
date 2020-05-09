package com.jaffa.rpc.lib.callbacks;

public interface Callback<T> {

    void onSuccess(String key, T result);

    void onError(String key, Throwable exception);
}
