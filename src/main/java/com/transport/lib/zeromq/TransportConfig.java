package com.transport.lib.zeromq;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@Configuration
@ComponentScan({"com.transport"})
@EnableAspectJAutoProxy(proxyTargetClass = true)
public class TransportConfig {

    @Bean
    public TransportService transportService(){
        return new TransportService();
    }
}
