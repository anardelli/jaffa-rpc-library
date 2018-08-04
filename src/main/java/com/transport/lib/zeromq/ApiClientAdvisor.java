package com.transport.lib.zeromq;

import org.aopalliance.aop.Advice;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractPointcutAdvisor;
import org.springframework.aop.support.StaticMethodMatcherPointcut;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.stereotype.Component;
import java.lang.reflect.Method;

@Component
@SuppressWarnings("unused")
public class ApiClientAdvisor extends AbstractPointcutAdvisor {

    private static final long serialVersionUID = 1L;

    private final MethodInterceptor interceptor;
    private final StaticMethodMatcherPointcut pointcut = new ApiClientAnnotationOnClassOrInheritedInterfacePointcut();

    public ApiClientAdvisor() {
        super();
        this.interceptor = (MethodInvocation invocation) -> {
            Command command = new Command();
            command.setServiceClass(invocation.getMethod().getDeclaringClass().getInterfaces()[0].getName());
            command.setMethodName(invocation.getMethod().getName());
            command.setArgs(invocation.getArguments());
            if(invocation.getMethod().getParameterCount() != 0){
                String[] methodArgs = new String[invocation.getMethod().getParameterCount()];
                Class[]  argClasses = invocation.getMethod().getParameterTypes();
                for(int i = 0; i < methodArgs.length; i++){
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