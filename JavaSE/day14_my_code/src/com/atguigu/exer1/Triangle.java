package com.atguigu.exer1;

public class Triangle {
    private double a, b, c;

    public Triangle(double a, double b, double c) {
        if (a > 0 && b > 0 && c > 0 && (a + b > c) && (b + c > a) && (a + c > b)) {
            this.a = a;
            this.b = b;
            this.c = c;
        } else {
            throw new IllegalArgumentException("不构成三角形");
        }
    }
}
