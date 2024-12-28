package com.atguigu.exer6;


public class CreditCard extends DepositCard {
    private double creditLimit;
    private double creditCurrent;

    public CreditCard() {
    }

    public CreditCard(String account, double balance, double creditLimit, double credit) {
        super(account, balance);
        this.creditLimit = creditLimit;
        this.creditCurrent = credit;
    }

    @Override
    public void withdraw(double money) {
        System.out.println("取款" + money);
        if (money < 0 || money > (getBalance() + creditCurrent)) {
            System.out.println("取款失败，取款金额已超过可透支额度");
        } else if (money <= getBalance()) {
            setBalance(getBalance() - money);
            System.out.println("取款成功，余额：" + getBalance() + " 剩余透支额度:" + creditCurrent);
        } else {
            money -= getBalance();
            setBalance(0);
            creditCurrent -= money;
            System.out.println("余额不足，已透支，余额：" + getBalance() + " 剩余透支额度:" + creditCurrent);
        }
    }

    public void save(double money) {
        System.out.println("存款" + money);
        if (money <= 0) {
            System.out.println("存款失败，存款金额不能为负");
        } else if (money <= (creditLimit - creditCurrent)) {
            creditCurrent += money;
            System.out.println("存款成功，余额为：" + getBalance() + " 透支额度为：" + creditCurrent);
        } else {
            money -= (creditLimit - creditCurrent);
            creditCurrent = creditLimit;
            setBalance(getBalance() + money);
            System.out.println("存款成功，余额为：" + getBalance() + " 透支额度为：" + creditCurrent);
        }
    }

    public String toString() {
        return super.toString() + " 剩余透支额度：" + creditCurrent;
    }
}
