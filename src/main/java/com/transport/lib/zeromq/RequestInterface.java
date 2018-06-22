package com.transport.lib.zeromq;

public interface RequestInterface<T> {

    RequestInterface<T> withTimeout(int timeout);

    T execute();

    RequestInterface<T> onModule(String moduleId);
}
