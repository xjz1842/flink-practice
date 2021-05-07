package com.learn.flink;

public class Person {

    public String name;
    public Integer age;
    public Person() {};
    public Person(String name, Integer age) {
        this.age = age;
        this.name = name;
    };

    public String toString() {
        return this.name.toString() + ": age " + this.age.toString();
    };

}
