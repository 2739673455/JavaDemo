package com.atguigu.exer7;

public abstract class Graphic {
    public abstract double area();

    public abstract double perimeter();

    public String toString() {
        return "，面积：" + area() + "，周长：" + perimeter();
    }
}
