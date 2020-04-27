package com.transport.lib.security;

/*
    Represents TicketProvider that generates SecurityTicket for authorization and authentication
 */
public interface TicketProvider {
    SecurityTicket getTicket();
}
