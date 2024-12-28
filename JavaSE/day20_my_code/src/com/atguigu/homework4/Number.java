package com.atguigu.homework4;

public class Number {
    private int odd = 1;
    private int even = 2;

    public synchronized void printOdd() throws InterruptedException {
        for (int i = 0; i < 5; ++i) {
            System.out.println(Thread.currentThread().getName() + " : " + odd);
            odd += 2;
        }
        Thread.sleep(500);
    }

    public synchronized void printEven() throws InterruptedException {
        for (int i = 0; i < 5; ++i) {
            System.out.println(Thread.currentThread().getName() + " : " + even);
            even += 2;
        }
        Thread.sleep(500);
    }

}
