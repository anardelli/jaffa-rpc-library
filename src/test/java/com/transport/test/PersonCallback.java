package com.transport.test;

import com.transport.lib.callbacks.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersonCallback implements Callback<Person> {

    private static Logger logger = LoggerFactory.getLogger(PersonCallback.class);

    @Override
    public void onSuccess(String key, Person result) {
        logger.info("Key: " + key);
        logger.info("Result: " + result);
    }

    @Override
    public void onError(String key, Throwable exception) {
        logger.error("Exception during async call:", exception);
    }
}