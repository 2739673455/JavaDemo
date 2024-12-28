package com.atguigu;

public class Homework1 {
    public static void main(String[] args) {
        new Thread() {
            @Override
            public void run() {
                for (int i = 2; i <= 100; i += 2) {
                    System.out.println("even:" + i);
                }
            }
        }.start();

        for (int i = 1; i <= 100; i += 2) {
            System.out.println("odd:" + i);
        }
    }
}
