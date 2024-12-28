package com.atguigu.thread;

import java.util.Random;

public class Task1 implements Runnable {
    private int total;

    public Task1(int total) {
        this.total = total;
    }

    public void run() {
        while (total > 0) {
            try {
                sell();
//                synchronized (getClass()) {
//                    if (total > 0) {
//                        total--;
//                        System.out.println(Thread.currentThread().getName() + "卖出一张票,还剩" + total + "张");
//                    }
//                }
                Thread.sleep(new Random().nextInt(50));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public synchronized void sell() {
        if (total > 0) {
            total--;
            System.out.print("\r" + Thread.currentThread().getName() + "卖出一张票,还剩" + total + "张");
        }
    }
}
