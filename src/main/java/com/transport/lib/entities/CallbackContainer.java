package com.transport.lib.entities;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/*
    Class-container for passing result of async method invocation to Callback
 */
@NoArgsConstructor
@Getter
@Setter
@ToString
public class CallbackContainer {
    // Unique user-provided key for identifying original request in Callback
    private String key;
    // Fully-qualified classname for callback implementation
    private String listener;
    // Result object
    private Object result;
    // Fully-qualified classname for result object's class
    private String resultClass;
}
