package com.transport.lib.common;

/*
    Represents TicketProvider that generates SecurityTicket for authorization and authentication
 */
public interface TicketProvider {
    SecurityTicket getTicket();
}
