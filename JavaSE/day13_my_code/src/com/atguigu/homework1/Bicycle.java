package com.atguigu.homework1;

public class Bicycle extends Vehicle {
    Bicycle(int wheels) {
        super(wheels);
    }

    Bicycle() {
    }

    @Override
    public void drive() {
        System.out.println("脚踏双轮自行车，悠哉悠哉往前走");
    }
}
