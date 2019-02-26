package com.transport.test;

import com.transport.lib.common.ClientEndpoints;
import com.transport.lib.common.ServerEndpoints;
import com.transport.lib.common.TransportConfig;
import org.springframework.context.annotation.*;

@Configuration
@ComponentScan
@Import(TransportConfig.class)
@SuppressWarnings("all")
public class MainConfig {

    @Bean
    ServerEndpoints serverEndpoints(){ return new ServerEndpoints(PersonServiceImpl.class, ClientServiceImpl.class); }

    @Bean
    ClientEndpoints clientEndpoints(){ return new ClientEndpoints(ClientServiceTransport.class, PersonServiceTransport.class); }
}
