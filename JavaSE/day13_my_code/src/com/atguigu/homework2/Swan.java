package com.atguigu.homework2;

public class Swan extends Bird implements Flyable, Swimming {
    public void eat() {
        System.out.println("天鹅吃水生植物的根茎和种子、水生昆虫、螺类和小鱼");
    }

    public void fly() {
        System.out.println("天鹅展翅高飞，天南海北任我游");
    }

    public void swim() {
        System.out.println("天鹅把羽毛洗的锃亮，顺便捉条鱼");
    }

}
