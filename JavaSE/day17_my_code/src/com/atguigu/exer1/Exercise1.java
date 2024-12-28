package com.atguigu.exer1;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.function.Predicate;

public class Exercise1 {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<>();
        list.add("hello");
        list.add("java");
        list.add("world");
        list.add("atguigu");
        list.add("love");
        list.add("you");
        list.add("mom");
        list.add("dad");
        list.add("noon");

        for (String s : list) {
            System.out.print(s + " ");
        }
        System.out.println();


        Predicate<String> p = new Predicate<>() {
            @Override
            public boolean test(String o) {
                char[] c = o.toCharArray();
                boolean flag = true;
                for (int i = 0; i < c.length / 2; i++) {
                    if (c[i] != c[c.length - i - 1]) {
                        flag = false;
                        break;
                    }
                }
                return flag;
            }
        };

        list.removeIf(p);
        for (String s : list) {
            System.out.print(s + " ");
        }

        System.out.println();
        Iterator<String> it = list.iterator();
        while (it.hasNext()) {
            String word = it.next();
            System.out.println(word + " " + word.length());
        }
    }
}
