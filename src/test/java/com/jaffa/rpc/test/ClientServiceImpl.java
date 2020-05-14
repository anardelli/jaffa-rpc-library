package com.jaffa.rpc.test;

import com.jaffa.rpc.lib.annotations.ApiServer;
import com.jaffa.rpc.lib.entities.RequestContext;
import com.jaffa.rpc.lib.zookeeper.Utils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ApiServer
public class ClientServiceImpl implements ClientService {

    @Override
    public void lol3(String message) {
        log.info("SOURCE MODULE ID: " + RequestContext.getSourceModuleId() + " MY MODULE ID: " + Utils.getRequiredOption("jaffa.rpc.module.id"));
        log.info("TICKET: " + RequestContext.getTicket());
        log.info("lol3 " + message);
    }

    @Override
    public void lol4(String message) {
        log.info("SOURCE MODULE ID: " + RequestContext.getSourceModuleId() + " MY MODULE ID: " + Utils.getRequiredOption("jaffa.rpc.module.id"));
        log.info("TICKET: " + RequestContext.getTicket());
        log.info("lol4 " + message);
        try {
            Thread.sleep(11_000);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
