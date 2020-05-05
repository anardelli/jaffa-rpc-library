package com.transport.lib.zeromq;

import com.transport.lib.entities.Protocol;
import com.transport.lib.request.Sender;
import com.transport.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.SocketType;
import org.zeromq.ZMQ;

@Slf4j
public class ZeroMqRequestSender extends Sender {

    public static final ZMQ.Context context = ZMQ.context(10);

    @Override
    public byte[] executeSync(byte[] message) {
        long start = System.currentTimeMillis();
        byte[] response;
        try (ZMQ.Socket socket = context.socket(SocketType.REQ)) {
            socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.ZMQ));
            socket.send(message, 0);
            if (timeout != -1) {
                socket.setReceiveTimeOut((int) timeout);
            }
            response = socket.recv(0);
        }
        log.info(">>>>>> Executed sync request {} in {} ms", command.getRqUid(), System.currentTimeMillis() - start);
        return response;
    }

    @Override
    public void executeAsync(byte[] message) {
        long start = System.currentTimeMillis();
        try (ZMQ.Socket socket = context.socket(SocketType.REQ)) {
            socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.ZMQ));
            socket.send(message, 0);
            socket.recv(0);
        }
        log.info(">>>>>> Executed async request {} in {} ms", command.getRqUid(), System.currentTimeMillis() - start);
    }
}
