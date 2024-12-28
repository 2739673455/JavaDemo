package com.atguigu.exer8;

public class HourEmployee extends Employee {
    private double hours;
    private double moneyPerHour;

    public HourEmployee(String name, double moneyPerHour) {
        super(name);
        this.moneyPerHour = moneyPerHour;
    }

    public HourEmployee(String name, double moneyPerHour, double hours) {
        super(name);
        this.hours = hours;
        this.moneyPerHour = moneyPerHour;
    }

    @Override
    public double earning() {
        return hours * moneyPerHour;
    }

    @Override
    public String toString() {
        return super.toString() + " 时薪：" + moneyPerHour + " 工作时长：" + hours;
    }
}
