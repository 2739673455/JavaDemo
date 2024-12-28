package com.atguigu;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class Exercise2 {
    public static void main(String[] args) {
        Set<Integer> set = new HashSet<>();
        Random random = new Random();
        for (int i = 0; i < 10; i++) {
            set.add(random.nextInt(1, 50) * 2);
        }

        System.out.println(set.size());

        for (Integer i : set) {
            System.out.print(i + "  ");
        }
    }
}
