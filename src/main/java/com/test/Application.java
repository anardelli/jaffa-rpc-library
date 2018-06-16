package com.test;

import com.transport.lib.zeromq.ZeroRPCService;
import com.transport.lib.zookeeper.ZKUtils;
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
        ZeroRPCService service = new ZeroRPCService<>(new PersonServiceImpl());
        service.bind("tcp://" + ZKUtils.getServiceBindAddress());
        new Thread(service).start();

        ZKUtils.connect("localhost");
        ZKUtils.registerService(PersonService.class.getName());
        SpringApplication.run(Application.class, args);
    }

}
