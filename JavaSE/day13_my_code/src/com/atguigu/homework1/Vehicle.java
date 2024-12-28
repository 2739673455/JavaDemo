package com.atguigu.homework1;

public abstract class Vehicle {
    private int wheels;

    public Vehicle(int wheels) {
        this.wheels = wheels;
    }

    public Vehicle() {
    }

    public abstract void drive();

    public String toString() {
        return "轮子数量：" + wheels;
    }
}


