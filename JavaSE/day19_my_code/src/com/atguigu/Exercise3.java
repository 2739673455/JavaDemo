package com.atguigu;

public class Exercise3 {
    public static void main(String[] args) throws InterruptedException {
        Thread rabbit = new Thread() {
            public void run() {
                int length = 30;
                int time = 0;
                while (length > 0) {
                    length -= 10;
                    ++time;
                    if (length > 0) {
                        time += 10;
                    }
                }
                System.out.println("兔子用时:" + time);
            }
        };

        Thread tortoise = new Thread() {
            public void run() {
                int length = 30;
                int time = 0;
                while (length > 0) {
                    ++time;
                    length -= 1;
                    if (length % 10 == 0 && length > 0) {
                        ++time;
                    }
                }
                System.out.println("乌龟用时:" + time);
            }
        };
        rabbit.start();
        rabbit.join();
        tortoise.start();
        tortoise.join();
    }
}
