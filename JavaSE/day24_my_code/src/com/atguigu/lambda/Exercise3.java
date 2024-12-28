package com.atguigu.lambda;

import java.util.Random;
import java.util.function.Supplier;

public class Exercise3 {
    public static void main(String[] args) {
        Random random = new Random();
        Supplier<Integer> s = () -> random.nextInt(100);
        System.out.println(s + " -> " + s.get());
    }
}
