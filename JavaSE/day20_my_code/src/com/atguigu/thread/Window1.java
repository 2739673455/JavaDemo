package com.atguigu.thread;

public class Window1 {
    public static void main(String[] args) {
//        Task1 task1 = new Task1(100);
//        Thread thread1 = new Thread(task1);
//        Thread thread2 = new Thread(task1);
//        Thread thread3 = new Thread(task1);
//
//        thread1.start();
//        thread2.start();
//        thread3.start();

        Task2.setTotal(100);
        Thread task1 = new Task2();
        Thread task2 = new Task2();
        Thread task3 = new Task2();

        task1.start();
        task2.start();
        task3.start();

    }
}
