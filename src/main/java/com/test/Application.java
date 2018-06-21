package com.test;

import com.transport.lib.zeromq.ZeroRPCService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import java.net.UnknownHostException;

@Configuration
@ComponentScan({"com.test","com.transport"})
@EnableAutoConfiguration
public class Application {

    public static void main(String[] args) throws UnknownHostException {
        ZeroRPCService service = new ZeroRPCService();
        service.bind();
        SpringApplication.run(Application.class, args);
    }
}
