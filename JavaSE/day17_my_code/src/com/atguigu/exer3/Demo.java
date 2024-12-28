package com.atguigu.exer3;

public class Demo {
    public static void main(String[] args) {
        Coordinate<Double> coordinate1 = new Coordinate<>(1.0, 1.1);
        Coordinate<String> coordinate2 = new Coordinate<>("2.0", "2.1");
        System.out.println(coordinate1);
        System.out.println(coordinate2);
    }
}
