package com.atguigu.exer7;

//import java.time.Month

import java.util.Scanner;

public class Test {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        System.out.print("请输入年,月:");
        int year = input.nextInt();
        int month = input.nextInt();
        boolean leapYear = year % 4 == 0 && year % 100 != 0 || year % 400 == 0;
        Month m1 = Month.of(month);
        System.out.println(m1.getValue());
        System.out.println(m1.length(leapYear));
        System.out.println(m1.getDescription());
        input.close();
    }
}
