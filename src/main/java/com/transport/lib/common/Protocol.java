package com.transport.lib.common;

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

}
