package com.atguigu.graphiccompare;

import java.util.Arrays;
import java.util.Comparator;

public class Demo {
    public static void main(String[] args) {
        Rectangle[] r = new Rectangle[5];
        r[0] = new Rectangle(1, 2);
        r[1] = new Rectangle(2, 3);
        r[2] = new Rectangle(3, 4);
        r[3] = new Rectangle(4, 5);
        r[4] = new Rectangle(5, 6);

        Comparator<Rectangle> comparator = new Comparator<Rectangle>() {
            @Override
            public int compare(Rectangle r1, Rectangle r2) {
                return Double.compare(r1.area(), r2.area());
            }
        };

        show(r);
        Arrays.sort(r);
        System.out.println("按长度比较======================");
        show(r);
        Arrays.sort(r, comparator);
        System.out.println("按面积比较======================");
        show(r);
    }

    public static void show(Rectangle[] r) {
        for (Rectangle rectangle : r) {
            System.out.println(rectangle);
        }
    }
}

