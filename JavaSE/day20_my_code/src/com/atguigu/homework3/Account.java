package com.atguigu.homework3;

public class Account {
    private final String account;
    private double balance;

    public Account(String account, double balance) {
        this.account = account;
        this.balance = balance;
    }

    public String getAccount() {
        return account;
    }

    public double getBalance() {
        return balance;
    }

    @Override
    public String toString() {
        return "   账户：" + account + ", 余额：" + balance;
    }

    public synchronized void save(double money) {
        balance += money;
        this.notify();
    }

    public synchronized void withdraw(double money) throws InterruptedException {
        if (balance < money) {
            System.out.println("余额不足，等待中......");
            this.wait();
        } else {
            balance -= money;
        }
    }
}
