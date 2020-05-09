package com.transport.lib.entities;

import com.transport.lib.security.SecurityTicket;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;

@NoArgsConstructor
@Setter
@Getter
@ToString
public class Command implements Serializable {
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
    private long requestTime;
    private long localRequestTime;
}