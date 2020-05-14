package com.jaffa.rpc.lib.zookeeper;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZKClientConfig;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperConnection {

    private final CountDownLatch connectedSignal = new CountDownLatch(1);
    private ZooKeeper zoo;

    ZooKeeper connect(String host) throws IOException, InterruptedException {

        ZKClientConfig zkClientConfig = new ZKClientConfig();
        zkClientConfig.setProperty("zookeeper.clientCnxnSocket", System.getProperty("jaffa.rpc.zookeeper.clientCnxnSocket", "org.apache.zookeeper.ClientCnxnSocketNetty"));
        zkClientConfig.setProperty("zookeeper.client.secure", System.getProperty("jaffa.rpc.zookeeper.client.secure", "false"));
        if (Boolean.parseBoolean(System.getProperty("jaffa.rpc.zookeeper.client.secure", "false"))) {
            zkClientConfig.setProperty("zookeeper.ssl.keyStore.location", System.getProperty("jaffa.rpc.zookeeper.ssl.keyStore.location"));
            zkClientConfig.setProperty("zookeeper.ssl.keyStore.password", System.getProperty("jaffa.rpc.zookeeper.ssl.keyStore.password"));
            zkClientConfig.setProperty("zookeeper.ssl.trustStore.location", System.getProperty("jaffa.rpc.zookeeper.ssl.trustStore.location"));
            zkClientConfig.setProperty("zookeeper.ssl.trustStore.password", System.getProperty("jaffa.rpc.zookeeper.ssl.trustStore.password"));
        }
        zoo = new ZooKeeper(host, 5000, (watchedEvent -> {
            if (watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedSignal.countDown();
            }
        }), zkClientConfig);
        connectedSignal.await();
        return zoo;
    }

    public void close() throws InterruptedException {
        zoo.close();
    }
}
