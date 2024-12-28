package com.atguigu.homework1;

public class Tricycle extends Vehicle {
    Tricycle(int wheels) {
        super(wheels);
    }

    Tricycle() {
    }

    @Override
    public void drive() {
        System.out.println("脚踏三轮车，稳稳当当往前走");
    }
}
