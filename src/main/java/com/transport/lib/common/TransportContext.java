package com.transport.lib.common;

@SuppressWarnings("all")
/*
    Utility class for setting invocation metadata like client's module.id and security ticket in ThreadLocal variables
 */
public class TransportContext {

    // Client's module.id
    private static ThreadLocal<String> sourceModuleId = new ThreadLocal<>();
    // Security ticket instance
    private static ThreadLocal<SecurityTicket> securityTicketThreadLocal = new ThreadLocal<>();

    public static String getSourceModuleId() {
        return sourceModuleId.get();
    }

    public static void setSourceModuleId(String sourceModuleId) {
        TransportContext.sourceModuleId.set(sourceModuleId);
    }

    public static SecurityTicket getTicket() {
        return securityTicketThreadLocal.get();
    }

    public static void setSecurityTicketThreadLocal(SecurityTicket securityTicket) {
        TransportContext.securityTicketThreadLocal.set(securityTicket);
    }
}
