package com.atguigu.exer6;

public class Exercise6 {
    public static void main(String[] args) {
        CreditCard c1 = new CreditCard("456", 10000, 10000, 10000);
        c1.withdraw(9999);
        c1.withdraw(9999);
        c1.withdraw(9999);
        c1.save(99);
        c1.save(9999);
        c1.save(9999);
        System.out.println(c1);
    }
}