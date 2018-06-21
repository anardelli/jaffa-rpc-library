package com.transport.test;

import java.io.Serializable;

public class Address implements Serializable {

    private String zip;
    private String street;
    private String flat;
    private String city;

    public Address(String zip, String street, String flat, String city) {
        this.zip = zip;
        this.street = street;
        this.flat = flat;
        this.city = city;
    }

    public Address(){}

    public String getZip() {
        return zip;
    }

    public void setZip(String zip) {
        this.zip = zip;
    }

    public String getStreet() {
        return street;
    }

    public void setStreet(String street) {
        this.street = street;
    }

    public String getFlat() {
        return flat;
    }

    public void setFlat(String flat) {
        this.flat = flat;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "test.com.transport.test.Address{" +
                "zip='" + zip + '\'' +
                ", street='" + street + '\'' +
                ", flat='" + flat + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
}
