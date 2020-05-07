package com.transport.lib.annotations;

import com.transport.lib.security.TicketProvider;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ApiClient {
    Class<? extends TicketProvider> ticketProvider() default TicketProvider.class;
}
