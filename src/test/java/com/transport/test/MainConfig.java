package com.transport.test;

import com.transport.lib.configuration.TransportConfig;
import com.transport.lib.spring.ClientEndpoints;
import com.transport.lib.spring.ServerEndpoints;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ComponentScan
@Import(TransportConfig.class)
public class MainConfig {

    @Bean
    ServerEndpoints serverEndpoints() {
        return new ServerEndpoints(PersonServiceImpl.class, ClientServiceImpl.class);
    }

    @Bean
    ClientEndpoints clientEndpoints() {
        return new ClientEndpoints(ClientServiceTransport.class, PersonServiceTransport.class);
    }
}
