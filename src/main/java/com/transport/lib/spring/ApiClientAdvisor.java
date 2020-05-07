package com.transport.lib.spring;

import com.transport.lib.TransportService;
import com.transport.lib.annotations.ApiClient;
import com.transport.lib.entities.Command;
import com.transport.lib.entities.Protocol;
import com.transport.lib.exception.TransportSystemException;
import com.transport.lib.request.RequestImpl;
import com.transport.lib.security.TicketProvider;
import com.transport.lib.zookeeper.Utils;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
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
import java.net.UnknownHostException;
import java.util.UUID;

@Slf4j
@Component
@EqualsAndHashCode(callSuper = false)
public class ApiClientAdvisor extends AbstractPointcutAdvisor {

    private final transient MethodInterceptor interceptor;
    private final transient StaticMethodMatcherPointcut pointcut = new ApiClientAnnotationOnClassOrInheritedInterfacePointcut();

    @Autowired
    private transient ApplicationContext context;

    public ApiClientAdvisor() {
        super();
        this.interceptor = (MethodInvocation invocation) -> {
            Command command = new Command();
            setMetadata(command);
            command.setServiceClass(invocation.getMethod().getDeclaringClass().getInterfaces()[0].getName());
            ApiClient apiClient = invocation.getMethod().getDeclaringClass().getInterfaces()[0].getAnnotation(ApiClient.class);
            if (!apiClient.ticketProvider().equals(TicketProvider.class)) {
                TicketProvider ticketProvider = (TicketProvider) context.getBean(apiClient.ticketProvider());
                command.setTicket(ticketProvider.getTicket());
            }
            command.setMethodName(invocation.getMethod().getName());
            command.setArgs(invocation.getArguments());
            if (invocation.getMethod().getParameterCount() != 0) {
                String[] methodArgs = new String[invocation.getMethod().getParameterCount()];
                Class<?>[] argClasses = invocation.getMethod().getParameterTypes();
                for (int i = 0; i < methodArgs.length; i++) {
                    methodArgs[i] = argClasses[i].getName();
                }
                command.setMethodArgs(methodArgs);
            }
            return new RequestImpl<>(command);
        };
    }

    public void setMetadata(Command command) {
        try {
            if (Utils.getTransportProtocol().equals(Protocol.ZMQ))
                command.setCallBackZMQ(Utils.getZeroMQCallbackBindAddress());
            if (Utils.getTransportProtocol().equals(Protocol.HTTP))
                command.setCallBackZMQ(Utils.getHttpCallbackStringAddress());
        } catch (UnknownHostException e) {
            log.error("Error during metadata setting", e);
            throw new TransportSystemException(e);
        }
        command.setSourceModuleId(TransportService.getRequiredOption("module.id"));
        command.setRqUid(UUID.randomUUID().toString());
    }

    @Override
    public Pointcut getPointcut() {
        return this.pointcut;
    }

    @Override
    public Advice getAdvice() {
        return this.interceptor;
    }

    private static final class ApiClientAnnotationOnClassOrInheritedInterfacePointcut extends StaticMethodMatcherPointcut {
        @Override
        public boolean matches(Method method, Class<?> targetClass) {
            if (AnnotationUtils.findAnnotation(method, ApiClient.class) != null) {
                return true;
            }
            return AnnotationUtils.findAnnotation(targetClass, ApiClient.class) != null;
        }
    }
}