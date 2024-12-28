package com.atguigu.homework2;

public class Homework2 {
    public static void main(String[] args) {
        Number number = new Number();
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        number.printOddNum();
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                while (true) {
                    try {
                        number.printEvenNum();
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }.start();
    }
}
