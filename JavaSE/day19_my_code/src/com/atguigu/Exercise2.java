package com.atguigu;

import java.util.Scanner;

public class Exercise2 {
    public static void main(String[] args) throws InterruptedException {
        Thread chatThread = new Thread() {
            public void run() {
                Scanner input = new Scanner(System.in);

                while (true) {
                    System.out.print("结束？：");
                    String sign = input.next();
                    if ("y".equals(sign)) {
                        break;
                    }
                }
                input.close();
            }
        };


        for (int i = 10; i > 0; --i) {
            System.out.println(i);
            Thread.sleep(10);
            if (i == 5) {
                chatThread.start();
                chatThread.join();
            }
        }
    }
}
