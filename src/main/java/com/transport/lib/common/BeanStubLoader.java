package com.transport.lib.common;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.StubMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import java.util.HashSet;
import java.util.Set;

import static net.bytebuddy.matcher.ElementMatchers.any;

@Configuration
@SuppressWarnings("unused")
@DependsOn({"serverEndpoints", "clientEndpoints"})
public class BeanStubLoader implements BeanDefinitionRegistryPostProcessor {

    private static Logger logger = LoggerFactory.getLogger(BeanStubLoader.class);

    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {

        ClientEndpoints clientEndpoints = ((DefaultListableBeanFactory) registry).getBean(ClientEndpoints.class);
        ClassLoader cl = BeanStubLoader.class.getClassLoader();

        Set<Class<?>> annotated = new HashSet<>();
        for (Class client : clientEndpoints.getClientEndpoints()) {
            boolean isClient = client.isAnnotationPresent(ApiClient.class);
            logger.info("Client endpoint: " + client.getName() + " isClient: " + isClient);
            if (!isClient)
                throw new IllegalArgumentException("Class " + client.getName() + " is not annotated as ApiClient!");
            annotated.add(client);
        }

        for (Class<?> client : annotated) {
            if (client.isInterface()) {
                Class stubClass = new ByteBuddy()
                        .subclass(client)
                        .method(any())
                        .intercept(StubMethod.INSTANCE)
                        .make().load(cl, ClassLoadingStrategy.Default.INJECTION)
                        .getLoaded();
                BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(stubClass);
                registry.registerBeanDefinition(client.getSimpleName() + "Stub", builder.getBeanDefinition());
            }
        }
    }

    public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {
    }
}