package com.atguigu.homework4;

public class EvenPrint extends Thread {
    private String name;
    private Number number;

    public EvenPrint(String name, Number number) {
        super(name);
        this.number = number;
    }

    @Override
    public void run() {
        while (true) {
            try {
                number.printEven();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }
}
