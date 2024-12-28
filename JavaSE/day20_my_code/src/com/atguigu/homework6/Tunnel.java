package com.atguigu.homework6;

public class Tunnel {
    private int num = 1;

    public synchronized void pass() throws InterruptedException {
        System.out.println(Thread.currentThread().getName() + "开始通过隧道");
        Thread.sleep(1000);
        System.out.println(Thread.currentThread().getName() + "已经通过隧道，第" + (num++) + "名");
    }
}
