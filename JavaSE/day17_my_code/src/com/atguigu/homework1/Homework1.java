package com.atguigu.homework1;

import java.util.ArrayList;
import java.util.Random;
import java.util.function.Predicate;

public class Homework1 {
    public static void main(String[] args) {
        ArrayList<String> list = new ArrayList<String>();
        for (int i = 0; i < 10; ++i) {
            list.add(create());
        }
        System.out.println(list);

        list.removeIf(new Predicate<String>() {
            @Override
            public boolean test(String s) {
                for (char element : s.toCharArray()) {
                    if ("0123456789".contains(String.valueOf(element))) {
                        return false;
                    }
                }
                return true;
            }
        });
        System.out.println(list);
    }

    public static String create() {
        String str = "";
        Random random = new Random();
        for (int i = 0; i < 6; ++i) {
            int choice = random.nextInt(3);
            switch (choice) {
                case 0:
                    str += (char) (random.nextInt(26) + 65);
                    break;
                case 1:
                    str += (char) (random.nextInt(26) + 97);
                    break;
                case 2:
                    str += random.nextInt(10);
            }
        }
        return str;
    }
}
