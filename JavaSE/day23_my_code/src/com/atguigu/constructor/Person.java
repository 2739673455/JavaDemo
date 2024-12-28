package com.atguigu.constructor;

public class Person {
    private static String hp;
    private String name;
    private int age;

    private Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public Person() {
    }

    public static int max(int a, int b) {
        return a > b ? a : b;
    }

    public String getNameAge() {
        return name + age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "hp='" + hp + '\'' +
                ", name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
