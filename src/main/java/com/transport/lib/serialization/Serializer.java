package com.transport.lib.serialization;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Serializer {

    @Getter
    private static SerializationContext ctx;

    static {
        String currentSerializer = System.getProperty("transport.serializer", "kryo");
        switch (currentSerializer){
            case "kryo":
                ctx = new KryoPoolSerializer();
                break;
            case "java":
                ctx = new JavaSerializer();
                break;
            default:
                log.error("No known serializer defined");
                break;
        }
    }
}
