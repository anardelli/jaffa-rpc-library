package com.transport.test;

import lombok.*;

import java.io.Serializable;

@Getter
@Setter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class Person implements Serializable {
    private int id;
    private String name;
    private String email;
    private String twitter;
    private Address address;
}