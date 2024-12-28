package com.atguigu.homework6;

public class Homework6 {
    public static void main(String[] args) {
        Tunnel tunnel = new Tunnel();
        for (int i = 0; i < 10; ++i) {
            new Thread(String.valueOf(i)) {
                @Override
                public void run() {
                    try {
                        tunnel.pass();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }.start();
        }
    }
}
