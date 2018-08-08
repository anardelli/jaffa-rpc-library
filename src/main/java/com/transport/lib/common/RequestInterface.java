package com.transport.lib.common;

public interface RequestInterface<T> {

    RequestInterface<T> withTimeout(int timeout);

    T executeSync();

    RequestInterface<T> onModule(String moduleId);

    void executeAsync(String key, Class listener);
}
