package com.atguigu.exer4runswim;

public class Tortoise implements Runner, Swimming {
    @Override
    public void run() {
        System.out.println("乌龟跑得快");
    }

    @Override
    public void swim() {
        System.out.println("乌龟游得快");
    }
}
