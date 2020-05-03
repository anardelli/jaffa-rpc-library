package com.transport.lib.zeromq;

import com.transport.lib.entities.Protocol;
import com.transport.lib.request.Sender;
import com.transport.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;
import org.zeromq.ZMQ;

@Slf4j
public class ZeroMqRequestSender extends Sender {

    @Override
    public byte[] executeSync(byte[] message) {
        long start = System.currentTimeMillis();
        // New ZeroMQ context with 1 thread
        ZMQ.Context context = ZMQ.context(1);
        // Open socket
        ZMQ.Socket socket = context.socket(ZMQ.REQ);
        // Get target server host:port
        socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.ZMQ));
        // Send Command to server
        socket.send(message, 0);
        // Set timeout if provided
        if (timeout != -1) {
            socket.setReceiveTimeOut((int) timeout);
        }
        // Wait for answer from server
        byte[] response = socket.recv(0);
        // Close socket and context
        Utils.closeSocketAndContext(socket, context);
        log.info(">>>>>> Executed sync request {} in {} ms", command.getRqUid(), System.currentTimeMillis() - start);
        return response;
    }

    @Override
    public void executeAsync(byte[] message) {
        long start = System.currentTimeMillis();
        // New ZeroMQ context with 1 thread
        ZMQ.Context context = ZMQ.context(1);
        // Open socket
        ZMQ.Socket socket = context.socket(ZMQ.REQ);
        // Get target server host:port
        socket.connect("tcp://" + Utils.getHostForService(command.getServiceClass(), moduleId, Protocol.ZMQ));
        // Send Command to server
        socket.send(message, 0);
        // Wait for "OK" message from server that means request was received and correctly deserialized
        socket.recv(0);
        Utils.closeSocketAndContext(socket, context);
        log.info(">>>>>> Executed async request {} in {} ms", command.getRqUid(), System.currentTimeMillis() - start);
    }
}
