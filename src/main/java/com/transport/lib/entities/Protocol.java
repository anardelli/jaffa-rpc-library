package com.transport.lib.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;

/*
    Enum represents all supported transport protocols - Kafka and ZeroMQ
 */
@AllArgsConstructor
@Getter
public enum Protocol {

    KAFKA("kafka", "Apache Kafka"),
    ZMQ("zmq", "ZeroMQ"),
    HTTP("http", "HTTP/1.1"),
    RABBIT("rabbit", "RabbitMQ");

    // Short protocol name used for service registration in ZooKeeper
    private final String shortName;

    // Full protocol name used in UI
    private final String fullName;

    public static Protocol getByName(String name) {
        for (Protocol protocol : Protocol.values())
            if (protocol.getShortName().equals(name)) return protocol;
        return null;
    }
}
