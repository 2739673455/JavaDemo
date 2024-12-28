package com.atguigu.exer12;

public class Account {
    private static double rate;
    private double balance;
    private String id;

    public static double getRate() {
        return rate;
    }

    public static void setRate(double rate) {
        Account.rate = rate;
    }

    public double getBalance() {
        return balance;
    }

    public void setBalance(double balance) {
        this.balance = balance;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Account() {
    }

    public Account(String id, double balance) {
        this.balance = balance;
        this.id = id;
    }

    public double annualInterest() {
        return balance * rate;
    }

    @Override
    public String toString() {
        return "账号：" + id + " 余额：" + balance + " 年利息：" + annualInterest();
    }
}
