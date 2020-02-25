package com.transport.test;

import com.transport.lib.common.ApiClient;
import com.transport.lib.common.RequestInterface;

@ApiClient(ticketProvider = TicketProviderImpl.class)
public interface PersonServiceTransport {

    public RequestInterface<Integer> add(String name, String email, Address address);

    public RequestInterface<Person> get(Integer id);

    public RequestInterface<Void> lol();

    public RequestInterface<Void> lol2(String message);

    public RequestInterface<String> getName();

    public RequestInterface<Person> testError();
}

