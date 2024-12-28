package com.atguigu.exer12;

public class Demo {
    public static void main(String[] args) {
        Account a1 = new Account("11111", 1000);
        Account a2 = new Account("22222", 2000);
        Account.setRate(0.035);
        System.out.println(a1);
        System.out.println(a2);
    }
}
