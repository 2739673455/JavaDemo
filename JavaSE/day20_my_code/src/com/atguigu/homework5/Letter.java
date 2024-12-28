package com.atguigu.homework5;

public class Letter {
    private char upperLetter = 'a';
    private char lowerLetter = 'A';

    public synchronized void printLowerLetter() throws InterruptedException {
        this.notify();
        for (int i = 0; i < 3; ++i) {
            System.out.println(Thread.currentThread().getName() + " " + (char) (upperLetter++));
            if (upperLetter >= 'z') {
                upperLetter = 'a';
            }
        }
        this.wait();
    }

    public synchronized void printUpperLetter() throws InterruptedException {
        this.notify();
        for (int i = 0; i < 3; ++i) {
            System.out.println(Thread.currentThread().getName() + " " + (char) (lowerLetter++));
            if (lowerLetter >= 'Z') {
                lowerLetter = 'A';
            }
        }
        this.wait();
    }
}
