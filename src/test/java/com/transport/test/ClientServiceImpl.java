package com.transport.test;

import com.transport.lib.common.ApiServer;
import com.transport.lib.common.TransportContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApiServer
public class ClientServiceImpl implements ClientService {

    private static Logger logger = LoggerFactory.getLogger(ClientServiceImpl.class);

    @Override
    public void lol3(String message) {
        logger.info("SOURCE MODULE ID: " + TransportContext.getSourceModuleId() + " MY MODULE ID: " + System.getProperty("module.id"));
        logger.info("lol3 " + message);
    }

    @Override
    public void lol4(String message) {
        logger.info("SOURCE MODULE ID: " + TransportContext.getSourceModuleId() + " MY MODULE ID: " + System.getProperty("module.id"));
        logger.info("lol4 " + message);
    }
}
