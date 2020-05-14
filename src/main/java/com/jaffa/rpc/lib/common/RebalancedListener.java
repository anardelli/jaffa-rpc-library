package com.jaffa.rpc.lib.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

@Slf4j
@SuppressWarnings("squid:S1444")
public class RebalancedListener implements ConsumerRebalanceListener {
    public static volatile long firstEvent = 0L;
    public static volatile long lastEvent = 0L;

    public static void waitForRebalanced() {
        while (true) {
            if (System.nanoTime() - lastEvent > 3000000000L) break;
        }
    }

    private static long updateTimes() {
        synchronized (RebalancedListener.class) {
            long current = System.nanoTime();
            if (firstEvent == 0) {
                firstEvent = current;
                log.info("First event at {}", current);
            }
            if (current > lastEvent)
                lastEvent = current;
            return current;
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        long current = updateTimes();
        log.info("onPartitionsRevoked {} {}", partitions, current);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        long current = updateTimes();
        log.info("onPartitionsAssigned {} {}", partitions, current);
    }
}
