package com.atguigu.homework1;

import java.time.LocalDateTime;
import java.util.Date;

public class Homework1 {
    public static void main(String[] args) {
        CurrentTimePrinter c1 = () -> System.out.println(System.currentTimeMillis());
        CurrentTimePrinter c2 = () -> System.out.println(new Date());
        CurrentTimePrinter c3 = () -> System.out.print("\r" + LocalDateTime.now());

        c1.printCurrentTime();
        c2.printCurrentTime();
        c3.printCurrentTime();
    }
}
