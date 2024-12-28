package com.atguigu.exer4;

import java.util.TreeSet;

public class Exercise4 {
    public static void main(String[] args) {
        TreeSet<Circle> circleSet = new TreeSet<>();
        circleSet.add(new Circle(1.2));
        circleSet.add(new Circle(3.2));
        circleSet.add(new Circle(4.2));
        circleSet.add(new Circle(2.2));
        circleSet.add(new Circle(3.2));

        System.out.println(circleSet);
    }
}
