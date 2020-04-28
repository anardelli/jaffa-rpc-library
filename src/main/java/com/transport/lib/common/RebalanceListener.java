package com.transport.lib.common;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/*
    Class responsible for counting time since last partitions assignment or revocation
    Both events are considered as "instability" so transport context will wait for 500 since last such event
 */
public class RebalanceListener implements ConsumerRebalanceListener {

    // Used only for displaying time for rebalance
    public static volatile long firstRebalance = 0L;
    // Used in TransportService.waitForRebalance() for waiting cluster stability
    public static volatile long lastRebalance = 0L;
    private static final Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    /*
        Wait for Kafka cluster to be rebalanced
     */
    public static void waitForRebalance() {
        long start = 0L;
        // Take last rebalance event
        long lastRebalance = RebalanceListener.lastRebalance;
        // Wait...
        while (true) {
            // Last rebalance time not changed
            if (RebalanceListener.lastRebalance == lastRebalance) {
                // Start counting time since that event
                if (start == 0L) {
                    start = System.currentTimeMillis();
                    // Wait for 500 ms since last event
                } else if (System.currentTimeMillis() - start > 500) break;
            } else {
                // Oops, new rebalance event, start waiting again
                start = 0L;
                lastRebalance = RebalanceListener.lastRebalance;
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        lastRebalance = System.currentTimeMillis();
        if (firstRebalance == 0L) firstRebalance = lastRebalance;
        logger.info("onPartitionsRevoked");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        lastRebalance = System.currentTimeMillis();
        if (firstRebalance == 0L) firstRebalance = lastRebalance;
        logger.info("onPartitionsAssigned");
    }

}
