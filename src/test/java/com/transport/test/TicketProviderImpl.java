package com.transport.test;

import com.transport.lib.common.SecurityTicket;
import com.transport.lib.common.TicketProvider;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class TicketProviderImpl implements TicketProvider {

    @Override
    public SecurityTicket getTicket() {
        return new SecurityTicket("user1", UUID.randomUUID().toString());
    }
}
