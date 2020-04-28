package com.transport.lib.common;

import com.transport.lib.entities.Command;
import com.transport.lib.exception.TransportExecutionTimeoutException;
import com.transport.lib.exception.TransportSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

/*
    Class responsible for passing "Transport execution timeout" to Callback implementations
    after timeout occurred during async remote method invocation
 */
public class FinalizationWorker {

    // Many threads add Command from Request to this map, then finalizer periodically query it
    // Command resides here until one of the following events occurs:
    // - One of ResponseReceiver classes received callback response from server with required callback key
    // - Timeout occurred - each Command contains asyncExpireTime field,
    //   which equals to call time + timeout if it was specified
    //   or call time + 60 minutes otherwise
    public static final ConcurrentMap<String, Command> eventsToConsume = new ConcurrentHashMap<>();
    // Required to control finalizer thread startup
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);
    private static final Logger logger = LoggerFactory.getLogger(FinalizationWorker.class);
    // One thread per TransportService context, it is used to call target async callbacks with
    // RuntimeException("Transport execution timeout") when asyncExpireTime is larger than current time (timeout occurred)
    private static final Thread finalizer = new Thread(() -> {
        logger.info("Finalizer thread started");
        countDownLatch.countDown();
        while (Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
            }
            // Get all Commands with expireTime > now()
            eventsToConsume.values().stream().filter(x -> x.getAsyncExpireTime() < System.currentTimeMillis()).forEach((Command command) -> {
                try {
                    // Necessary to eliminate the possibility of race between finalization thread and callback receiver thread
                    if (eventsToConsume.remove(command.getCallbackKey()) != null) {
                        logger.info("Finalization command {}", command);
                        // Get target Callback implementation
                        Class<?> callbackClass = Class.forName(command.getCallbackClass());
                        // And invoke Callback.onError() with new TransportExecutionTimeoutException()
                        Method method = callbackClass.getMethod("onError", String.class, Throwable.class);
                        method.invoke(callbackClass.getDeclaredConstructor().newInstance(), command.getCallbackKey(), new TransportExecutionTimeoutException());
                    }
                } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    logger.error("Error during finalization command: {}", command);
                }
            });
        }
        logger.info("Finalizer thread stopped");
    });

    public static void startFinalizer() {
        finalizer.start();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Error during FinalizationWorker startup");
            throw new TransportSystemException(e);
        }
    }

    public static void stopFinalizer() {
        do {
            finalizer.interrupt();
        } while (finalizer.getState() != Thread.State.TERMINATED);
    }
}
