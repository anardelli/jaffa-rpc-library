package com.transport.lib.common;

import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractPointcutAdvisor;
import org.springframework.aop.support.StaticMethodMatcherPointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/*
    AOP advisor to intercept method invocations on @ApiClient transport proxies
 */
@Component
@SuppressWarnings("unused")
public class ApiClientAdvisor extends AbstractPointcutAdvisor {

    private final MethodInterceptor interceptor;
    private final StaticMethodMatcherPointcut pointcut = new ApiClientAnnotationOnClassOrInheritedInterfacePointcut();

    @Autowired
    private ApplicationContext context;

    public ApiClientAdvisor() {
        super();
        // Here we prepare and return new Request ready for execution
        this.interceptor = (MethodInvocation invocation) -> {
            Command command = new Command();
            // Prepare metadata like callBackZMQ, sourceModuleId, rqUid
            command.setMetadata();
            // Class with package which method we want to execute
            command.setServiceClass(invocation.getMethod().getDeclaringClass().getInterfaces()[0].getName());
            // Checking for TicketProvider implementation
            ApiClient apiClient = invocation.getMethod().getDeclaringClass().getInterfaces()[0].getAnnotation(ApiClient.class);
            if (!apiClient.ticketProvider().equals(void.class)) {
                // User defined custom TicketProvider, we should take it from context
                TicketProvider ticketProvider = (TicketProvider) context.getBean(apiClient.ticketProvider());
                // And then we generate ticket
                command.setTicket(ticketProvider.getTicket());
            }
            // Method we want to execute
            command.setMethodName(invocation.getMethod().getName());
            // Save arguments - all arguments must be serializable
            command.setArgs(invocation.getArguments());
            // Save argument's types as an array of fully-qualified class names
            if (invocation.getMethod().getParameterCount() != 0) {
                String[] methodArgs = new String[invocation.getMethod().getParameterCount()];
                Class[] argClasses = invocation.getMethod().getParameterTypes();
                for (int i = 0; i < methodArgs.length; i++) {
                    methodArgs[i] = argClasses[i].getName();
                }
                command.setMethodArgs(methodArgs);
            }
            // And here new Request is ready
            return new Request(command);
        };
    }

    @Override
    public Pointcut getPointcut() {
        return this.pointcut;
    }

    @Override
    public Advice getAdvice() {
        return this.interceptor;
    }

    private final class ApiClientAnnotationOnClassOrInheritedInterfacePointcut extends StaticMethodMatcherPointcut {
        @Override
        public boolean matches(Method method, Class<?> targetClass) {
            // Apply AOP only for classes with @ApiClient annotation
            if (AnnotationUtils.findAnnotation(method, ApiClient.class) != null) {
                return true;
            }
            return AnnotationUtils.findAnnotation(targetClass, ApiClient.class) != null;
        }
    }
}