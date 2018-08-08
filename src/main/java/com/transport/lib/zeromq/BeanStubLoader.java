package com.transport.lib.zeromq;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.StubMethod;
import org.reflections.Reflections;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.BeanDefinitionRegistryPostProcessor;
import org.springframework.context.annotation.Configuration;
import java.util.Set;

import static com.transport.lib.zeromq.TransportService.getRequiredOption;
import static net.bytebuddy.matcher.ElementMatchers.any;

@Configuration
@SuppressWarnings("unused")
public class BeanStubLoader implements BeanDefinitionRegistryPostProcessor {
    public void postProcessBeanDefinitionRegistry(BeanDefinitionRegistry registry) throws BeansException {
        String serviceRoot = getRequiredOption("service.root");
        if(serviceRoot == null) throw new IllegalArgumentException("Property service.root was not set");
        ClassLoader cl = BeanStubLoader.class.getClassLoader();
        Reflections reflections = new Reflections(serviceRoot);
        Set<Class<?>> annotated = reflections.getTypesAnnotatedWith(ApiClient.class);
        for(Class<?> client : annotated){
            if(client.isInterface()){
                Class stubClass = new ByteBuddy()
                        .subclass(client)
                        .method(any())
                        .intercept(StubMethod.INSTANCE)
                        .make().load(cl, ClassLoadingStrategy.Default.INJECTION)
                        .getLoaded();
                BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(stubClass);
                registry.registerBeanDefinition(client.getSimpleName()+"Stub", builder.getBeanDefinition());
            }
        }
    }
    public void postProcessBeanFactory(ConfigurableListableBeanFactory configurableListableBeanFactory) throws BeansException {
    }
}