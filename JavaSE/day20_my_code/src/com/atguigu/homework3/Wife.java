package com.atguigu.homework3;

import java.util.Random;

public class Wife extends Thread {
    private String name;
    private Account account;

    public Wife(String name, Account account) {
        this.name = name;
        this.account = account;
    }

    @Override
    public void run() {
        while (true) {
            try {
                double money = new Random().nextDouble(100, 10000);
                account.withdraw(money);
                System.out.println("妻子取出" + money + account);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
