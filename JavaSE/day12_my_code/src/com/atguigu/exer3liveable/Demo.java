package com.atguigu.exer3liveable;

public class Demo {
    public static void main(String[] args) {
        Animal a1 = new Animal();
        Plant p1 = new Plant();
        a1.eat();
        a1.breathe();
        a1.sleep();
        p1.eat();
        p1.breathe();
        LiveAble.drink();
    }
}
