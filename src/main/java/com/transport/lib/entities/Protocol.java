package com.transport.lib.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;

/*
    Enum represents all supported transport protocols - Kafka and ZeroMQ
 */
@AllArgsConstructor
@Getter
public enum Protocol {

    KAFKA("kafka"),
    ZMQ("zmq");

    // Short protocol name used for service registration in ZooKeeper
    private String shortName;

    public static Protocol getByName(String name) {
        for (Protocol protocol : Protocol.values())
            if (protocol.getShortName().equals(name)) return protocol;
        return null;
    }
}
