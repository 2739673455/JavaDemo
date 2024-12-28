package com.atguigu.homework3;

import java.util.Random;

public class Husband extends Thread {
    private String name;
    private Account account;

    public Husband(String name, Account account) {
        this.name = name;
        this.account = account;
    }

    @Override
    public void run() {
        while (true) {
            double money = new Random().nextDouble(100, 10000);
            account.save(money);
            System.out.println("丈夫存入" + money + account);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
