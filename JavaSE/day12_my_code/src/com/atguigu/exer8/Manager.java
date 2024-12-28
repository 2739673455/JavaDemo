package com.atguigu.exer8;

public class Manager extends SalaryEmployee {
    private double bonusRate;

    public double getBonusRate() {
        return bonusRate;
    }

    public void setBonusRate(double bonusRate) {
        this.bonusRate = bonusRate;
    }

    public Manager(String name, double salary, MyDate date, double bonusRate) {
        super(name, salary, date);
        this.bonusRate = bonusRate;
    }

    public Manager(String name, double salary, int year, int month, int day, double bonusRate) {
        super(name, salary, year, month, day);
        this.bonusRate = bonusRate;
    }

    @Override
    public double earning() {
        return this.getSalary() * (bonusRate + 1);
    }

    @Override
    public String toString() {
        return super.toString() + " 奖金比例：" + bonusRate;
    }
}
