package com.atguigu.homework3;

public class Homework3 {
    public static void main(String[] args) {
        Account account = new Account("1", 500.0);
        Husband husband = new Husband("张三", account);
        Wife wife = new Wife("李四", account);

        husband.start();
        wife.start();
    }
}
