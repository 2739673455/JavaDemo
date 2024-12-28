package com.atguigu.homework1;

public class Test {
    public static void main(String[] args) {
        Monocycle m = new Monocycle(1);
        Bicycle b = new Bicycle(2);
        Tricycle t = new Tricycle(3);
        m.drive();
        b.drive();
        t.drive();
        System.out.println(m);
        System.out.println(b);
        System.out.println(t);
    }
}
