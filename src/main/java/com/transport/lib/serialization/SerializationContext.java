package com.transport.lib.serialization;

public interface SerializationContext {

    byte[] serialize(Object obj);

    byte[] serializeWithClass(Object obj);

    Object deserializeWithClass(byte[] serialized);

    <T> T deserialize(byte[] serialized, Class<T> clazz);
}
