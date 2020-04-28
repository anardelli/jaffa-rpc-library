package com.transport.test;

import com.transport.lib.annotations.ApiServer;
import com.transport.lib.entities.RequestContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApiServer
public class ClientServiceImpl implements ClientService {

    private static final Logger logger = LoggerFactory.getLogger(ClientServiceImpl.class);

    @Override
    public void lol3(String message) {
        logger.info("SOURCE MODULE ID: " + RequestContext.getSourceModuleId() + " MY MODULE ID: " + System.getProperty("module.id"));
        logger.info("TICKET: " + RequestContext.getTicket());
        logger.info("lol3 " + message);
    }

    @Override
    public void lol4(String message) {
        logger.info("SOURCE MODULE ID: " + RequestContext.getSourceModuleId() + " MY MODULE ID: " + System.getProperty("module.id"));
        logger.info("TICKET: " + RequestContext.getTicket());
        logger.info("lol4 " + message);
        try {
            Thread.sleep(11_000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
