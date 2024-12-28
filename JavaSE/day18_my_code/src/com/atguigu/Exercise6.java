package com.atguigu;

import java.util.LinkedHashSet;
import java.util.Scanner;

public class Exercise6 {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        LinkedHashSet<String> set = new LinkedHashSet<>();
        String str = "";
        while (!"stop".equals(str)) {
            str = input.next();
            set.add(str);
        }
        input.close();


        System.out.println(set);
    }
}
