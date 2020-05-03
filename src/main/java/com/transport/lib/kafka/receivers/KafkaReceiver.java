package com.transport.lib.kafka.receivers;

import com.transport.lib.TransportService;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.text.MessageFormat;
import java.util.ArrayList;

/*
    Class responsible for managing threads used by Kafka...Receivers
 */
@Slf4j
public abstract class KafkaReceiver implements Closeable, Runnable {

    /*
        Thread pool for Kafka message receivers one of the following types:
        - async requests
        - async responses
        - sync requests
     */
    private final ArrayList<Thread> threads = new ArrayList<>(TransportService.getBrokersCount());

    // Method starts one thread (consumer) per Kafka broker (partition)
    void startThreadsAndWait(Runnable runnable) {
        for (int i = 0; i < TransportService.getBrokersCount(); i++) {
            threads.add(new Thread(runnable));
        }
        // Start all threads
        threads.forEach(Thread::start);

        // Join all threads
        threads.forEach(x -> {
            try {
                x.join();
            } catch (InterruptedException e) {
                log.error(MessageFormat.format("Can not join thread {0} in {1}", x.getName(), this.getClass().getSimpleName()), e);
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
            log.info("Thread {} from {} terminated", thread.getName(), this.getClass().getSimpleName());
        }
        log.info("{} terminated", this.getClass().getSimpleName());
    }
}
