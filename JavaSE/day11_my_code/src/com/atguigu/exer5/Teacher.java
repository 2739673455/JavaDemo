package com.atguigu.exer5;

public class Teacher extends Person {
    private int salary;

    public int getSalary() {
        return salary;
    }

    public void setSalary(int salary) {
        this.salary = salary;
    }

    public Teacher() {
    }

    public Teacher(String name, int age, char gender, int salary) {
        super(name, age, gender);
        this.salary = salary;
    }

    public String toString() {
        return super.toString() + " 薪资:" + salary;
    }
}
