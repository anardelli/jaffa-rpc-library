package com.transport.lib.zeromq;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.net.UnknownHostException;

@Configuration
@ComponentScan({"com.transport"})
@EnableAspectJAutoProxy(proxyTargetClass = true)
public class TransportConfig {

    @Bean
    public ZeroRPCService zeroRPCService(){
        ZeroRPCService service = new ZeroRPCService();
        try {
            service.bind();
        }catch (UnknownHostException e){
            e.printStackTrace();
        }
        return service;
    }
}
