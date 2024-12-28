package com.atguigu.exer8;

public class Employee {
    private String name;

    public Employee() {
    }

    public Employee(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public double earning() {
        return 0.0;
    }

    @Override
    public String toString() {
        return "姓名：" + name + " 实发工资：" + earning();
    }
}
