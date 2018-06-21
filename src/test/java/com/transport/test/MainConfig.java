package com.transport.test;

import com.transport.lib.zeromq.TransportConfig;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ComponentScan({"com.transport.test"})
@EnableAutoConfiguration
@Import(TransportConfig.class)
public class MainConfig {

}
