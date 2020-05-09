package com.transport.lib.entities;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.io.Serializable;

@AllArgsConstructor
@Getter
public enum Protocol implements Serializable {

    KAFKA("kafka", "Apache Kafka"),
    ZMQ("zmq", "ZeroMQ"),
    HTTP("http", "HTTP/1.1"),
    RABBIT("rabbit", "RabbitMQ");

    private final String shortName;
    private final String fullName;

    public static Protocol getByName(String name) {
        for (Protocol protocol : Protocol.values())
            if (protocol.getShortName().equals(name)) return protocol;
        return null;
    }
}
