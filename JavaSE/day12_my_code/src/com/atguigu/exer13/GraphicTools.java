package com.atguigu.exer13;

public class GraphicTools {
    public static int compare(Graphic s1, Graphic s2) {
        if (s1.area() > s2.area()) {
            return 1;
        } else if (s1.area() < s2.area()) {
            return -1;
        } else {
            return 0;
        }
    }

    public static boolean equals(Graphic s1, Graphic s2) {
        return s1.area() == s2.area() && s1.perimeter() == s2.perimeter();
    }

    public static void sort(Graphic[] arr) {
        for (int i = 0; i < arr.length - 1; i++) {
            for (int j = 0; j < arr.length - i - 1; j++) {
                if ((arr[j].area() > arr[j + 1].area()) || ((arr[j].area() == arr[j + 1].area()) && (arr[j].perimeter() > arr[j + 1].perimeter()))) {
                    Graphic temp = arr[j];
                    arr[j] = arr[j + 1];
                    arr[j + 1] = temp;
                }
            }
        }
    }

    public static void print(Graphic[] arr) {
        for (Graphic g : arr) {
            System.out.println(g);
        }
    }
}
