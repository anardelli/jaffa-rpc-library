package com.transport.lib.common;

import java.io.Serializable;

public class SecurityTicket implements Serializable {

    private String user;
    private String token;

    public SecurityTicket() {
    }

    public SecurityTicket(String user, String token) {
        this.user = user;
        this.token = token;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return "SecurityTicket{" +
                "user='" + user + '\'' +
                ", token='" + token + '\'' +
                '}';
    }
}
