package com.jaffa.rpc.lib.common;

import com.jaffa.rpc.lib.entities.Command;
import com.jaffa.rpc.lib.exception.TransportExecutionTimeoutException;
import com.jaffa.rpc.lib.exception.TransportSystemException;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FinalizationWorker {

    @Getter
    private static final ConcurrentMap<String, Command> eventsToConsume = new ConcurrentHashMap<>();
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final Thread finalizer = new Thread(() -> {
        log.info("Finalizer thread started");
        countDownLatch.countDown();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                log.info("Finalizer thread was interrupted");
                Thread.currentThread().interrupt();
            }
            eventsToConsume.values().stream().filter(x -> x.getAsyncExpireTime() < System.currentTimeMillis()).forEach((Command command) -> {
                try {
                    if (eventsToConsume.remove(command.getCallbackKey()) != null) {
                        long start = System.nanoTime();
                        log.info("Finalization request {}", command.getRqUid());
                        Class<?> callbackClass = Class.forName(command.getCallbackClass());
                        Method method = callbackClass.getMethod("onError", String.class, Throwable.class);
                        method.invoke(callbackClass.getDeclaredConstructor().newInstance(), command.getCallbackKey(), new TransportExecutionTimeoutException());
                        log.info("Finalization request {} took {}ns", command.getRqUid(), (System.nanoTime() - start));
                    }
                } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    log.error("Error during finalization command: {}", command);
                }
            });
        }
        log.info("Finalizer thread stopped");
    });

    @SuppressWarnings("squid:S2142")
    public static void startFinalizer() {
        finalizer.start();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("Error during FinalizationWorker startup");
            throw new TransportSystemException(e);
        }
    }

    public static void stopFinalizer() {
        do {
            finalizer.interrupt();
        } while (finalizer.getState() != Thread.State.TERMINATED);
    }
}
