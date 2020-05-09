package com.jaffa.rpc.lib.entities;

import com.jaffa.rpc.lib.security.SecurityTicket;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RequestContext {

    private static final ThreadLocal<String> sourceModuleId = new ThreadLocal<>();
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
