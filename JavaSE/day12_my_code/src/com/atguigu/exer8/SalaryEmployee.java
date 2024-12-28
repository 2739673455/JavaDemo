package com.atguigu.exer8;

public class SalaryEmployee extends Employee {
    private double salary;
    private MyDate birthday;

    public double getSalary() {
        return salary;
    }

    public void setSalary(double salary) {
        this.salary = salary;
    }

    public MyDate getBirthday() {
        return birthday;
    }

    public void setBirthday(MyDate birthday) {
        this.birthday = birthday;
    }

    public SalaryEmployee() {
    }

    public SalaryEmployee(String name, double salary, MyDate birthday) {
        super(name);
        this.salary = salary;
        this.birthday = birthday;
    }

    public SalaryEmployee(String name, double salary, int year, int month, int day) {
        super(name);
        this.salary = salary;
        this.birthday = new MyDate(year, month, day);
    }

    @Override
    public double earning() {
        return salary;
    }

    @Override
    public String toString() {
        return super.toString() + " 生日" + birthday;
    }
}
