package com.atguigu.homework3;

public class Designer extends Programmer {
    private double bonus;

    public Designer() {
    }

    public Designer(String id, String name, int age, double salary, String job, double bonus) {
        super(id, name, age, salary, job);
        this.bonus = bonus;
    }

    public double getBonus() {
        return bonus;
    }

    public void setBonus(double bonus) {
        this.bonus = bonus;
    }

    @Override
    public String toString() {
        return super.toString() + "\t" + bonus;
    }
}
