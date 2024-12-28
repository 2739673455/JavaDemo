package com.atguigu.homework2;

public class Juicer implements Runnable {
    private Fruit f;

    public Juicer() {
    }

    public Juicer(Fruit f) {
        this.f = f;
    }

    @Override
    public void run() {
        f.squeeze();
    }
}
