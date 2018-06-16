package com.transport.lib.zeromq;

public interface RequestInterface<T> {

    public RequestInterface<T> withTimeout(int timeout);

    public T execute();
}
