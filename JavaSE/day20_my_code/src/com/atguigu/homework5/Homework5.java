package com.atguigu.homework5;

public class Homework5 {
    public static void main(String[] args) {
        Letter letter = new Letter();

        new Thread("小写字母") {
            @Override
            public void run() {
                while (true) {
                    try {
                        letter.printLowerLetter();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }.start();

        new Thread("大写字母") {
            @Override
            public void run() {
                while (true) {
                    try {
                        letter.printUpperLetter();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }.start();
    }
}
