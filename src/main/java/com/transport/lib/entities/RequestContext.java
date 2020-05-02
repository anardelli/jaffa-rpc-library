package com.transport.lib.entities;

import com.transport.lib.security.SecurityTicket;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/*
    Utility class for setting invocation metadata like client's module.id and security ticket in ThreadLocal variables
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RequestContext {

    // Client's module.id
    private static final ThreadLocal<String> sourceModuleId = new ThreadLocal<>();
    // Security ticket instance
    private static final ThreadLocal<SecurityTicket> securityTicketThreadLocal = new ThreadLocal<>();

    public static String getSourceModuleId() {
        return sourceModuleId.get();
    }

    public static void setSourceModuleId(String sourceModuleId) {
        RequestContext.sourceModuleId.set(sourceModuleId);
    }

    public static SecurityTicket getTicket() {
        return securityTicketThreadLocal.get();
    }

    public static void setSecurityTicket(SecurityTicket securityTicket) {
        RequestContext.securityTicketThreadLocal.set(securityTicket);
    }
}
