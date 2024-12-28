package com.atguigu;

import java.util.LinkedHashSet;
import java.util.Scanner;

public class Homework2 {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        char[] str = input.next().toCharArray();
        LinkedHashSet<Character> cSet = new LinkedHashSet<>();
        for (int i = 0; i < str.length; i++) {
            cSet.add(str[i]);
        }
        System.out.println(cSet);
        input.close();
    }
}
