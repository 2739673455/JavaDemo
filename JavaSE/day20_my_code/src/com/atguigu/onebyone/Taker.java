package com.atguigu.onebyone;

public class Taker extends Thread {
    private String name;
    private Workbench workbench;

    public Taker(String name, Workbench workbench) throws InterruptedException {
        super(name);
        this.workbench = workbench;
    }

    public void run() {
        while (true) {
            try {
                workbench.take();
//                Thread.sleep(new Random().nextInt(1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
