package com.atguigu.onebyone;

public class Workbench {
    private int count;

    public void put() throws InterruptedException {
        ++count;
        Thread.sleep(500);
        System.out.println(Thread.currentThread().getName() + ": " + count);
    }

    public void take() throws InterruptedException {
        --count;
        Thread.sleep(500);
        System.out.println(Thread.currentThread().getName() + ": " + count);
    }
}
