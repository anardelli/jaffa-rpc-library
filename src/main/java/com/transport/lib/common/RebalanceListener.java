package com.transport.lib.common;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

public class RebalanceListener implements ConsumerRebalanceListener {

    static volatile long firstRebalance = 0L;
    static volatile long lastRebalance = 0L;

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        lastRebalance = System.currentTimeMillis();
        if(firstRebalance == 0L) firstRebalance = lastRebalance;
        System.out.println("onPartitionsRevoked");
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        lastRebalance = System.currentTimeMillis();
        if(firstRebalance == 0L) firstRebalance = lastRebalance;
        System.out.println("onPartitionsAssigned");
    }
}
