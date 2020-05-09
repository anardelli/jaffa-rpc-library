package com.jaffa.rpc.test;

import com.jaffa.rpc.lib.security.SecurityTicket;
import com.jaffa.rpc.lib.security.TicketProvider;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class TicketProviderImpl implements TicketProvider {

    @Override
    public SecurityTicket getTicket() {
        return new SecurityTicket("user1", UUID.randomUUID().toString());
    }
}
