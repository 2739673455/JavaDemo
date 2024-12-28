package com.atguigu;

import java.util.Random;
import java.util.TreeSet;

public class Homework3 {
    public static void main(String[] args) {
        TreeSet<Integer> redNum = new TreeSet<>();
        Random rand = new Random();
        while (redNum.size() < 6) {
            redNum.add(rand.nextInt(1, 34));
        }
        System.out.print(redNum);
        System.out.println(" " + rand.nextInt(1, 17));
    }
}
