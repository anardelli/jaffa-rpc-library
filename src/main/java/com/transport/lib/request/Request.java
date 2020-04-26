package com.transport.lib.request;

import com.transport.lib.callbacks.Callback;

/*
    Represents abstract request
 */
public interface Request<T> {

    Request<T> withTimeout(int timeout);

    T executeSync();

    Request<T> onModule(String moduleId);

    void executeAsync(String key, Class<? extends Callback<T>> listener);
}
