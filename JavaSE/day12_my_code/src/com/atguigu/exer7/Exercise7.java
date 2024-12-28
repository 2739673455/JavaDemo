package com.atguigu.exer7;

public class Exercise7 {
    public static void main(String[] args) {
        Graphic[] arrGraphic = new Graphic[3];
        arrGraphic[0] = new Circle(2);
        arrGraphic[1] = new Rectangle(3, 6);
        arrGraphic[2] = new Triangle(3, 4, 5);
        bubbleSortOfArea(arrGraphic);
        for (Graphic g : arrGraphic) {
            System.out.println(g);
        }
    }

    public static void bubbleSortOfArea(Graphic[] arrGraphic) {
        for (int i = 0; i < arrGraphic.length - 1; i++) {
            for (int j = 0; j < arrGraphic.length - i - 1; j++) {
                if (arrGraphic[j].area() > arrGraphic[j + 1].area()) {
                    Graphic temp = arrGraphic[j];
                    arrGraphic[j] = arrGraphic[j + 1];
                    arrGraphic[j + 1] = temp;
                }
            }
        }
    }
}
