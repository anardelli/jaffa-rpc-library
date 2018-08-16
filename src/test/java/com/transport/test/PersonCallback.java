package com.transport.test;

import com.transport.lib.common.Callback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PersonCallback implements Callback<Person> {

    private static Logger logger = LoggerFactory.getLogger(PersonCallback.class);

    @Override
    public void callBack(String key, Person result) {
        logger.info("Key: " + key);
        logger.info("Result: " + result);
    }

    @Override
    public void callBackError(String key, Throwable exception) {
        logger.error("Exception during async call:", exception);
    }
}