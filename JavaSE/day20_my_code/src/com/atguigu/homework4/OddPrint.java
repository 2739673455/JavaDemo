package com.atguigu.homework4;

public class OddPrint extends Thread {
    private String name;
    private Number number;

    public OddPrint(String name, Number number) {
        super(name);
        this.number = number;
    }

    @Override
    public void run() {
        while (true) {
            try {
                number.printOdd();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
