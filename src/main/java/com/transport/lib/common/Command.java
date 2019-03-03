package com.transport.lib.common;

import com.transport.lib.zookeeper.Utils;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

@SuppressWarnings("all")
@NoArgsConstructor
@Setter
@Getter
@ToString
public class Command {

    private static Logger logger = LoggerFactory.getLogger(Command.class);

    private String serviceClass;
    private String methodName;
    private String[] methodArgs;
    private Object[] args;
    private String callbackClass;
    private String callbackKey;
    private String callBackZMQ;
    private String sourceModuleId;
    private String rqUid;
    private SecurityTicket ticket;
    private long asyncExpireTime;

    public void setMetadata(){
        try {
            this.callBackZMQ = Utils.getZeroMQCallbackBindAddress();
        }catch (Exception e){
            logger.error("Error during metadata setting", e);
        }
        this.sourceModuleId = TransportService.getRequiredOption("module.id");
        this.rqUid = UUID.randomUUID().toString();
    }
}