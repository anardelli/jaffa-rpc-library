package com.transport.lib.common;

public enum Protocol {
    KAFKA("kafka"),
    ZMQ("zmq");
    private String regName;
    Protocol(String regName){ this.regName = regName; }
    public String getRegName() { return regName; }
}
