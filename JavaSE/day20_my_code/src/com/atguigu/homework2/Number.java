package com.atguigu.homework2;

public class Number {
    private int num = 1;
    private boolean odd = true;

    public Number() {
    }

    public synchronized void printOddNum() throws InterruptedException {
        while (!odd) {
            this.wait();
        }
        System.out.println(Thread.currentThread().getName() + " " + num++);
        odd = !odd;
        this.notify();
    }

    public synchronized void printEvenNum() throws InterruptedException {
        while (odd) {
            this.wait();
        }
        System.out.println(Thread.currentThread().getName() + " " + num++);
        odd = !odd;
        this.notify();
    }
}
