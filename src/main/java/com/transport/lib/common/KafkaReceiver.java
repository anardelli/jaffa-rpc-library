package com.transport.lib.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;

import static com.transport.lib.common.TransportService.brokersCount;

@SuppressWarnings("WeakerAccess")
public abstract class KafkaReceiver implements Closeable, Runnable {

    private static Logger logger = LoggerFactory.getLogger(KafkaReceiver.class);

    // "thread pool" for Kafka message receivers of the following types:
    // - async requests
    // - async responses
    // - sync requests
    protected final ArrayList<Thread> threads = new ArrayList<>(brokersCount);

    // method starts one thread (consumer) per Kafka broker (partition)
    protected void startThreadsAndWait(Runnable runnable) {
        for (int i = 0; i < brokersCount; i++) {
            threads.add(new Thread(runnable));
        }
        threads.forEach(Thread::start);
        threads.forEach(x -> {
            try {
                x.join();
            } catch (Exception ignore) {
            }
        });
    }

    public void close() {
        for (Thread thread : this.threads) {
            do {
                thread.interrupt();
            } while (thread.getState() != Thread.State.TERMINATED);
        }
        logger.info(this.getClass().getSimpleName() + " terminated");
    }
}
