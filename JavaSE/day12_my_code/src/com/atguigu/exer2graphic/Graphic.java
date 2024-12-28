package com.atguigu.exer2graphic;

public abstract class Graphic {
    public abstract double area();

    public abstract double perimeter();

    public String toString() {
        return "，面积：" + area() + "，周长：" + perimeter();
    }
}
