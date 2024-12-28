package com.atguigu.homework2;

public class Penguin extends Bird implements Swimming {
    public void eat() {
        System.out.println("企鹅吃南极磷虾");
    }

    public void swim() {
        System.out.println("企鹅下海捉虾");
    }
}
