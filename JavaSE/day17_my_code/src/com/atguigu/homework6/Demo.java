package com.atguigu.homework6;

import java.util.Arrays;

public class Demo {
    public static void main(String[] args) {
        MyArrayList list = new MyArrayList();
        list.add(1);
        list.add(2);
        list.add(3);
        list.add(4);
        list.add(5);
        list.add(6);
        list.add(7);
        list.add(8);
        list.add(9);
        list.add(10);

        System.out.println(list.get(5));
        System.out.println(Arrays.toString(list.toArray()));

    }
}
