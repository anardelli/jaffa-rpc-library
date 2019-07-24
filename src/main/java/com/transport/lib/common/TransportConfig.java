package com.transport.lib.common;

import org.springframework.context.annotation.*;

/*
    Base Spring configuration for our transport library
    Must be imported in your Spring configuration
 */
@Configuration
@ComponentScan({"com.transport"})
@EnableAspectJAutoProxy(proxyTargetClass = true)
public class TransportConfig {

    @Bean(destroyMethod = "close")
    @DependsOn({"serverEndpoints", "clientEndpoints"})
    public TransportService transportService() {
        return new TransportService();
    }
}
