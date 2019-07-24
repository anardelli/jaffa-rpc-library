package com.transport.lib.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/*
    Class-container for passing Throwable as a result from server to client
 */
@SuppressWarnings("all")
@NoArgsConstructor
@AllArgsConstructor
@Getter
public class ExceptionHolder {
    // Stacktrace of exception occurred on server side
    private String stackTrace;
}
