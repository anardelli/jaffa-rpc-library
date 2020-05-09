package com.transport.lib.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoPool;

import java.io.ByteArrayOutputStream;

public class KryoPoolSerializer implements KryoContext {
    public static final KryoContext serializer = newKryoContextFactory();
    private static final int DEFAULT_BUFFER = 1024 * 100;
    private final KryoPool pool;

    private KryoPoolSerializer() {
        pool = new KryoPool.Builder(Kryo::new).softReferences().build();
    }

    public static KryoContext newKryoContextFactory() {
        return new KryoPoolSerializer();
    }

    @Override
    public byte[] serialize(Object obj) {
        Output output = new Output(new ByteArrayOutputStream(), DEFAULT_BUFFER);
        Kryo kryo = pool.borrow();
        kryo.writeObject(output, obj);
        byte[] serialized = output.toBytes();
        pool.release(kryo);
        return serialized;
    }

    @Override
    public byte[] serializeWithClass(Object obj) {
        Output output = new Output(new ByteArrayOutputStream(), DEFAULT_BUFFER);
        Kryo kryo = pool.borrow();
        kryo.writeClassAndObject(output, obj);
        byte[] serialized = output.toBytes();
        pool.release(kryo);
        return serialized;
    }

    @Override
    public Object deserializeWithClass(byte[] serialized) {
        Object obj;
        Kryo kryo = pool.borrow();
        Input input = new Input(serialized);
        obj = kryo.readClassAndObject(input);
        pool.release(kryo);
        return obj;
    }

    @Override
    public <T> T deserialize(byte[] serialized, Class<T> clazz) {
        T obj;
        Kryo kryo = pool.borrow();
        Input input = new Input(serialized);
        obj = kryo.readObject(input, clazz);
        pool.release(kryo);
        return obj;
    }
}
