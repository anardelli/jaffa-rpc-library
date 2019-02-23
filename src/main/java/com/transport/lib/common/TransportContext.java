package com.transport.lib.common;

@SuppressWarnings("all")
public class TransportContext {

    private static ThreadLocal<String> sourceModuleId = new ThreadLocal<>();
    private static ThreadLocal<SecurityTicket> securityTicketThreadLocal = new ThreadLocal<>();

    public static String getSourceModuleId() {
        return sourceModuleId.get();
    }

    public static void setSourceModuleId(String sourceModuleId) {
        TransportContext.sourceModuleId.set(sourceModuleId);
    }

    public static SecurityTicket getTicket() { return securityTicketThreadLocal.get();}

    public static void setSecurityTicketThreadLocal(SecurityTicket securityTicket){
        TransportContext.securityTicketThreadLocal.set(securityTicket);
    }
}
