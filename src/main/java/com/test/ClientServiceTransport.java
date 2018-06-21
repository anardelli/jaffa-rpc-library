package com.test;

import com.transport.lib.zeromq.ApiClient;
import com.transport.lib.zeromq.RequestInterface;

@ApiClient
public interface ClientServiceTransport {

    public RequestInterface<Void> lol3(String message);

    public RequestInterface<Void> lol4(String message);
}

