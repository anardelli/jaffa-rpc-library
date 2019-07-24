package com.transport.lib.common;

/*
    Enum represents all supported transport protocols - Kafka and ZeroMQ
 */
public enum Protocol {
    KAFKA("kafka"),
    ZMQ("zmq");
    private String regName;

    Protocol(String regName) {
        this.regName = regName;
    }

    public String getRegName() {
        return regName;
    }
}
