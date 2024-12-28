package com.atguigu.exer2graphic;

public class Demo {
    public static void main(String[] args) {
        Graphic[] arrGraphic = new Graphic[3];
        arrGraphic[0] = new Circle(2);
        arrGraphic[1] = new Rectangle(3, 6);
        arrGraphic[2] = new Triangle(3, 4, 5);
        for (Graphic g : arrGraphic) {
            System.out.println(g);
        }
    }
}
