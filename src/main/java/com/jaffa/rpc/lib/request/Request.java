package com.jaffa.rpc.lib.request;

import com.jaffa.rpc.lib.callbacks.Callback;

public interface Request<T> {
    Request<T> withTimeout(long timeout);

    T executeSync();

    Request<T> onModule(String moduleId);

    void executeAsync(String key, Class<? extends Callback<T>> listener);
}
