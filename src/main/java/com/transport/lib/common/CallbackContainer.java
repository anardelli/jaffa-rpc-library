package com.transport.lib.common;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@SuppressWarnings("WeakerAccess, unused")
@NoArgsConstructor
@Getter
@Setter
@ToString
public class CallbackContainer {
    private String key;
    private String listener;
    private Object result;
    private String resultClass;

}
