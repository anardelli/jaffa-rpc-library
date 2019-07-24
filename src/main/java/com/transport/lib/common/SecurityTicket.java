package com.transport.lib.common;

import lombok.*;

import java.io.Serializable;

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
