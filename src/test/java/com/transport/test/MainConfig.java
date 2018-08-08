package com.transport.test;

import com.transport.lib.common.TransportConfig;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@ComponentScan
@Import(TransportConfig.class)
public class MainConfig {

}
