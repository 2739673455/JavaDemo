package com.atguigu.exer11;

import com.atguigu.exer7.Graphic;

public class Triangle extends Graphic {
    private final double a, b, c;

    public Triangle(double a, double b, double c) {
        if (a > 0 && b > 0 && c > 0 && a + b > c && b + c > a && a + c > b) {
            this.a = a;
            this.b = b;
            this.c = c;
        } else {
            System.out.println(a + "," + b + "," + c + "不构成三角形");
            this.a = 0;
            this.b = 0;
            this.c = 0;
        }
    }

    public double getA() {
        return a;
    }

    public double getB() {
        return b;
    }

    public double getC() {
        return c;
    }


    @Override
    public double area() {
        double p = (a + b + c) / 2;
        return Math.sqrt(p * (p - a) * (p - b) * (p - c));
    }

    @Override
    public double perimeter() {
        return a + b + c;
    }

    public String toString() {
        return "边长：" + a + "," + b + "," + c + super.toString();
    }
}
