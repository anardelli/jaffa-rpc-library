package com.transport.lib.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("all")
public class FinalizationWorker {

    private static Logger logger = LoggerFactory.getLogger(FinalizationWorker.class);

    // Many threads add Command from Request to this map, then finalizer periodically query it
    // Command resides here until one of the following events occurs:
    // - One of ResponseReceiver classes received callback response from server with required callback key
    // - Timeout occurred - each Command contains asyncExpireTime field,
    //   which equals to call time + timeout if it was specified
    //   or call time + 60 minutes otherwise
    public static final ConcurrentHashMap<String, Command> eventsToConsume = new ConcurrentHashMap<>();

    // Required to control finalizer thread startup
    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    // One thread per TransportService context, it is used to call target async callbacks with
    // RuntimeException("Transport execution timeout") when asyncExpireTime is larger than current time (timeout occurred)
    private static final Thread finalizer = new Thread(() -> {
        logger.info("Finalizer thread started");
        countDownLatch.countDown();
        try{
            while(true){
                Thread.sleep(5);
                eventsToConsume.values().stream().filter(x -> x.getAsyncExpireTime() < System.currentTimeMillis()).forEach((Command command)->{
                    try{
                        // necessary to eliminate the possibility of race between finalization thread and callback receiver thread
                        if(eventsToConsume.remove(command.getCallbackKey()) != null){
                            logger.info("Finalization command " + command);
                            Class callbackClass = Class.forName(command.getCallbackClass());
                            Method method = callbackClass.getMethod("callBackError", String.class, Throwable.class );
                            method.invoke(callbackClass.newInstance(), command.getCallbackKey(), new RuntimeException("Transport execution timeout"));
                        }
                    }catch (Exception e){
                        logger.error("Error during finalization command: " + command);
                    }
                });
            }
        }catch (Exception e){
            logger.warn("Finalizer thread has stopped");
        }
    });

    public static void startFinalizer() {
        finalizer.start();
        try {
            countDownLatch.await();
        }catch (InterruptedException e){
            logger.error("Error during FinalizationWorker startup");
        }
    }

    public static void stopFinalizer(){
        do { finalizer.interrupt(); }while(finalizer.getState() != Thread.State.TERMINATED);
    }
}
