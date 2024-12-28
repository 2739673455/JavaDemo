package com.atguigu.lambda;

import java.util.ArrayList;
import java.util.HashMap;

public class Exercise2 {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        for (char i = 'a'; i <= 'z'; ++i) {
            list.add(String.valueOf(i));
        }
        list.forEach((c) -> System.out.print(c));
        System.out.println();

        HashMap<Integer, String> map = new HashMap<>();
        map.put(1, "Java");
        map.put(2, "C");
        map.put(3, "Python");
        map.put(4, "C++");
        map.put(5, "C#");
        map.forEach((k, v) -> System.out.println(k + " : " + v));
    }
}
