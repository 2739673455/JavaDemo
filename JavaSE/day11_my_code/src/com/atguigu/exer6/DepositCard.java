package com.atguigu.exer6;

public class DepositCard {
    private String account;
    private double balance;

    public DepositCard() {
    }

    public DepositCard(String account, double balance) {
        this.account = account;
        this.balance = balance;
    }

    public String getAccount() {
        return account;
    }

    public void setAccount(String account) {
        this.account = account;
    }

    public double getBalance() {
        return balance;
    }

    public void setBalance(double balance) {
        this.balance = balance;
    }

    public void withdraw(double money) {
        if (money <= 0 || money > balance) {
            System.out.println("取款金额错误，无法取款");
        } else {
            balance -= money;
            System.out.println("取款已完成，余额：" + balance);
        }
    }

    public void save(double money) {
        if (money <= 0) {
            System.out.println("存款金额不能为负");
        } else {
            balance += money;
            System.out.println("存款已完成，余额：" + balance);
        }
    }

    public String toString() {
        return "账户：" + account + "\t余额：" + balance;
    }
}
