package com.jaffa.rpc.lib.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

@Slf4j
public class RebalanceListener implements ConsumerRebalanceListener {

    public static volatile long firstRebalance = 0L;
    public static volatile long lastRebalance = 0L;

    public static void waitForRebalance() {
        long start = 0L;
        long lastRebalance = RebalanceListener.lastRebalance;
        while (true) {
            if (RebalanceListener.lastRebalance == lastRebalance) {
                if (start == 0L) {
                    start = System.currentTimeMillis();
                } else if (System.currentTimeMillis() - start > 500) break;
            } else {
                start = 0L;
                lastRebalance = RebalanceListener.lastRebalance;
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        lastRebalance = System.currentTimeMillis();
        if (firstRebalance == 0L) firstRebalance = lastRebalance;
        log.info("onPartitionsRevoked {}", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        lastRebalance = System.currentTimeMillis();
        if (firstRebalance == 0L) firstRebalance = lastRebalance;
        log.info("onPartitionsAssigned {}", partitions);
    }
}
