package com.atguigu.exer1;

public class Test {
    public static void main(String[] args) {
        try {
            Triangle t1 = new Triangle(2, 1, 3);
            System.out.println(t1);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }
}
