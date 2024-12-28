package com.atguigu.homework1;

public class Monocycle extends Vehicle {
    Monocycle(int wheels) {
        super(wheels);
    }

    Monocycle() {
    }

    @Override
    public void drive() {
        System.out.println("脚踏独轮车，摇摇摆摆往前走");
    }
}
