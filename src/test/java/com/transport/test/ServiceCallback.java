package com.transport.test;

import com.transport.lib.common.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceCallback implements Callback<Void> {

    private static Logger logger = LoggerFactory.getLogger(ServiceCallback.class);

    @Override
    public void callBack(String key, Void result) {
        logger.info("Key: " + key);
        logger.info("Result: " + result);
    }

    @Override
    public void callBackError(String key, Throwable exception) {
        logger.error("Exception during async call:", exception);
    }
}
