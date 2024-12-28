package com.atguigu.homework2;

public class Test {
    public static void main(String[] args) {
        Bird[] arr = new Bird[3];
        arr[0] = new Penguin();
        arr[1] = new Swan();
        arr[2] = new Chicken();

        for (Bird iterator : arr) {
            iterator.eat();
            if (iterator instanceof Flyable) {
                ((Flyable) iterator).fly();
            }
            if (iterator instanceof Swimming) {
                ((Swimming) iterator).swim();
            }
        }
    }
}
