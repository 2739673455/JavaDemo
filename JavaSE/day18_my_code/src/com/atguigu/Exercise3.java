package com.atguigu;

import java.util.HashSet;
import java.util.Random;

public class Exercise3 {
    public static void main(String[] args) {
        Random random = new Random();
        HashSet<Integer> set = new HashSet<>();
        while (set.size() < 10) {
            set.add(random.nextInt(1, 50) * 2);
        }


        System.out.println(set);
    }
}
