package com.transport.test;

import com.transport.lib.security.SecurityTicket;
import com.transport.lib.security.TicketProvider;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class TicketProviderImpl implements TicketProvider {

    @Override
    public SecurityTicket getTicket() {
        return new SecurityTicket("user1", UUID.randomUUID().toString());
    }
}
