package com.atguigu.thread;

import java.util.Random;

public class Task2 extends Thread {
    private static int total;

    public static int getTotal() {
        return total;
    }

    public static void setTotal(int total) {
        Task2.total = total;
    }

    public void run() {
        while (total > 0) {
            try {
//                sell();
                synchronized (getClass()) {
                    if (total > 0) {
                        Thread.sleep(new Random().nextInt(50));
                        total--;
                        System.out.println(Thread.currentThread().getName() + "卖出一张票,还剩" + total + "张");
                    }
                }
//                Thread.sleep(new Random().nextInt(500));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

//    public synchronized void sell() {
//        if (total > 0) {
//            total--;
//            System.out.println(Thread.currentThread().getName() + "卖出一张票,还剩" + total + "张");
//        }
//    }
}
