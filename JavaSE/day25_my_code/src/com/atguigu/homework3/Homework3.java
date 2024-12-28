package com.atguigu.homework3;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Homework3 {
    public static void main(String[] args) {
        ArrayList<String> list1 = new ArrayList<>();
        list1.add("教父");
        list1.add("肖申克的救赎");
        list1.add("辛德勒的名单");
        list1.add("公民凯恩");
        list1.add("卡萨布兰卡");
        list1.add("教父续集");
        list1.add("七武士");
        list1.add("星球大战");
        list1.add("美国美人");
        list1.add("飞跃疯人院");

        ArrayList<String> list2 = new ArrayList<>();
        list2.add("霸王别姬");
        list2.add("大闹天宫");
        list2.add("鬼子来了");
        list2.add("大话西游");
        list2.add("活着");
        list2.add("饮食男女");
        list2.add("无间道");
        list2.add("天书奇谭");
        list2.add("哪吒脑海");
        list2.add("春光乍泄");

//        test1(list1);
//        test2(list2);
//        test3(list1, list2);
        test4(list1, list2);
    }


    public static void test1(ArrayList<String> list) {
        list.stream().limit(3).forEach(System.out::println);
    }

    public static void test2(ArrayList<String> list) {
        list.stream().skip(5).forEach(System.out::println);
    }

    public static void test3(ArrayList<String> list1, ArrayList<String> list2) {
        ArrayList<String> list3 = new ArrayList<>();
        list1.stream().limit(5).forEach(list3::add);
        list2.stream().limit(5).forEach(list3::add);
        list3.forEach(System.out::println);
    }

    public static void test4(ArrayList<String> list1, ArrayList<String> list2) {
//        ArrayList<Film> list4 = new ArrayList<>();
//        list1.stream().forEach((o) -> list4.add(new Film(o)));
//        list2.stream().forEach((o) -> list4.add(new Film(o)));
//        list4.forEach(System.out::println);

        Stream.concat(list1.stream(), list2.stream())
                .map(Film::new)
                .collect(Collectors.toList())
                .forEach(System.out::println);
    }
}
