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

@Component
@SuppressWarnings("unused")
public class ApiClientAdvisor extends AbstractPointcutAdvisor {

    private static final long serialVersionUID = 1L;
    private final MethodInterceptor interceptor;
    private final StaticMethodMatcherPointcut pointcut = new ApiClientAnnotationOnClassOrInheritedInterfacePointcut();
    @Autowired
    private ApplicationContext context;

    public ApiClientAdvisor() {
        super();
        this.interceptor = (MethodInvocation invocation) -> {
            Command command = new Command();
            command.setMetadata();
            command.setServiceClass(invocation.getMethod().getDeclaringClass().getInterfaces()[0].getName());

            ApiClient apiClient = invocation.getMethod().getDeclaringClass().getInterfaces()[0].getAnnotation(ApiClient.class);
            if (!apiClient.ticketProvider().equals(void.class)) {
                TicketProvider ticketProvider = (TicketProvider) context.getBean(apiClient.ticketProvider());
                command.setTicket(ticketProvider.getTicket());
            }
            command.setMethodName(invocation.getMethod().getName());
            command.setArgs(invocation.getArguments());
            if (invocation.getMethod().getParameterCount() != 0) {
                String[] methodArgs = new String[invocation.getMethod().getParameterCount()];
                Class[] argClasses = invocation.getMethod().getParameterTypes();
                for (int i = 0; i < methodArgs.length; i++) {
                    methodArgs[i] = argClasses[i].getName();
                }
                command.setMethodArgs(methodArgs);
            }
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
            if (AnnotationUtils.findAnnotation(method, ApiClient.class) != null) {
                return true;
            }
            return AnnotationUtils.findAnnotation(targetClass, ApiClient.class) != null;
        }
    }
}