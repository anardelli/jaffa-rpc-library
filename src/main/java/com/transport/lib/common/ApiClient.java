package com.transport.lib.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/*
    This annotation is used in transport-maven-plugin generator
    to mark interfaces that will be generated using server @Api interfaces
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface ApiClient {
    // User-provided TicketProvider implementation
    Class<?> ticketProvider() default void.class;
}
