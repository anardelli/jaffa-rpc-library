package com.transport.test;

import com.transport.lib.common.ApiClient;
import com.transport.lib.common.RequestInterface;

@ApiClient(ticketProvider = TicketProviderImpl.class)
public interface ClientServiceTransport {

    public RequestInterface<Void> lol3(String message);

    public RequestInterface<Void> lol4(String message);
}

