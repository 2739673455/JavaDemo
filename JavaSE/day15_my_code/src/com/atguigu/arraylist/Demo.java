package com.atguigu.arraylist;

import java.util.ArrayList;
import java.util.Arrays;

public class Demo {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<String>();
        list.add("hello");
        list.add("java");
        list.add("atguigu");
        list.add("chai");
        list.add("lin");
        list.add("hello");
        list.add("yan");

        list.set(1, "尚硅谷");
        System.out.println(list.indexOf("hello"));
        System.out.println(list.lastIndexOf("hello"));
        System.out.println(list.get(4));

        list.remove(3);
        list.remove("chai");

        Object[] all = list.toArray();
        System.out.println(Arrays.toString(all));

    }
}