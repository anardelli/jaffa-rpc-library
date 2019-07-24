package com.transport.lib.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;

import static com.transport.lib.common.TransportService.brokersCount;

/*
    Class responsible for managing threads used by Kafka...Receivers
 */
@SuppressWarnings("WeakerAccess")
public abstract class KafkaReceiver implements Closeable, Runnable {

    private static Logger logger = LoggerFactory.getLogger(KafkaReceiver.class);

    /*
        Thread pool for Kafka message receivers one of the following types:
        - async requests
        - async responses
        - sync requests
     */
    protected final ArrayList<Thread> threads = new ArrayList<>(brokersCount);

    // Method starts one thread (consumer) per Kafka broker (partition)
    protected void startThreadsAndWait(Runnable runnable) {
        for (int i = 0; i < brokersCount; i++) {
            threads.add(new Thread(runnable));
        }
        // Start all threads
        threads.forEach(Thread::start);

        // Join all threads
        threads.forEach(x -> {
            try {
                x.join();
            } catch (InterruptedException e) {
                logger.error("Can't join thread " + x.getName() + " in " + this.getClass().getSimpleName(), e);
            }
        });
    }

    // Will be called from TransportService.close() during Spring context destruction
    @Override
    public void close() {
        // Stop all threads
        for (Thread thread : this.threads) {
            do {
                thread.interrupt();
            } while (thread.getState() != Thread.State.TERMINATED);
        }
        logger.info(this.getClass().getSimpleName() + " terminated");
    }
}
