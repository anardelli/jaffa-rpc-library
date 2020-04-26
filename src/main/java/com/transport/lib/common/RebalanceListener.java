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

    private static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    // Used only for displaying time for rebalance
    public static volatile long firstRebalance = 0L;
    // Used in TransportService.waitForRebalance() for waiting cluster stability
    public static volatile long lastRebalance = 0L;

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
