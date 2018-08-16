package com.transport.lib.common;

public class TransportContext {

    private static ThreadLocal<String> sourceModuleId = new ThreadLocal<>();

    public static String getSourceModuleId() {
        return sourceModuleId.get();
    }

    public static void setSourceModuleId(String sourceModuleId) {
        TransportContext.sourceModuleId.set(sourceModuleId);
    }
}
