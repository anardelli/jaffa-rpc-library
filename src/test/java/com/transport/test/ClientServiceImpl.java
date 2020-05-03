package com.transport.test;

import com.transport.lib.annotations.ApiServer;
import com.transport.lib.entities.RequestContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApiServer
public class ClientServiceImpl implements ClientService {

    @Override
    public void lol3(String message) {
        log.info("SOURCE MODULE ID: " + RequestContext.getSourceModuleId() + " MY MODULE ID: " + System.getProperty("module.id"));
        log.info("TICKET: " + RequestContext.getTicket());
        log.info("lol3 " + message);
    }

    @Override
    public void lol4(String message) {
        log.info("SOURCE MODULE ID: " + RequestContext.getSourceModuleId() + " MY MODULE ID: " + System.getProperty("module.id"));
        log.info("TICKET: " + RequestContext.getTicket());
        log.info("lol4 " + message);
        try {
            Thread.sleep(11_000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
