package com.atguigu.exer13;

public class Exercise11 {
    public static void main(String[] args) {
        Graphic[] arrGraphic = new Graphic[3];
        arrGraphic[0] = new Circle(2);
        arrGraphic[1] = new Rectangle(3, 6);
        arrGraphic[2] = new Triangle(3, 4, 5);
        System.out.println(GraphicTools.compare(arrGraphic[1], arrGraphic[2]));
        System.out.println(GraphicTools.equals(arrGraphic[1], arrGraphic[2]));
        GraphicTools.print(arrGraphic);
        GraphicTools.sort(arrGraphic);
        System.out.println("===================");
        GraphicTools.print(arrGraphic);
    }
}
