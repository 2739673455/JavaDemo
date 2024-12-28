package com.atguigu.homework4;

public class Homework4 {
    public static void main(String[] args) {
        Number number = new Number();
        new OddPrint("奇数", number).start();
        new EvenPrint("偶数", number).start();
    }
}
