package com.transport.lib.configuration;

import com.transport.lib.TransportService;
import org.springframework.context.annotation.*;

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
