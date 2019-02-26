package com.transport.lib.common;

import lombok.*;

import java.io.Serializable;

@ToString
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class SecurityTicket implements Serializable {
    private String user;
    private String token;
}