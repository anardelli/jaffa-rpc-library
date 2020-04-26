package com.transport.lib.security;

import com.transport.lib.security.SecurityTicket;

/*
    Represents TicketProvider that generates SecurityTicket for authorization and authentication
 */
public interface TicketProvider {
    SecurityTicket getTicket();
}
