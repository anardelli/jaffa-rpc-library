package com.transport.lib.security;

import lombok.*;

import java.io.Serializable;

/*
    Class-container for security information like login and token
 */
@ToString
@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class SecurityTicket implements Serializable {
    // User login
    private String user;
    // Generated security token
    private String token;
}
