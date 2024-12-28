package com.atguigu;

public class Exercise1 {
    public static void main(String[] args) throws InterruptedException {
        for (int i = 10; i > 0; --i) {
            System.out.println(i);
            Thread.sleep(100);
        }
        System.out.println("新年快乐");
    }
}
