package com.transport.lib.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

@SuppressWarnings("all")
public class FinalizationWorker {

    private static Logger logger = LoggerFactory.getLogger(FinalizationWorker.class);

    public static final ConcurrentHashMap<String, Command> eventsToConsume = new ConcurrentHashMap<>();

    private static final CountDownLatch countDownLatch = new CountDownLatch(1);

    private static final Thread finalizer = new Thread(() -> {
        logger.info("Finalizer thread started");
        countDownLatch.countDown();
        try{
            while(true){
                Thread.sleep(5);
                eventsToConsume.values().stream().filter(x -> x.getAsyncExpireTime() < System.currentTimeMillis()).forEach((Command command)->{
                    try{
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
