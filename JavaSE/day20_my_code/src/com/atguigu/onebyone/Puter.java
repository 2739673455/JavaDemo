package com.atguigu.onebyone;

public class Puter extends Thread {
    private String name;
    private Workbench workbench;

    public Puter(String name, Workbench workbench) throws InterruptedException {
        super(name);
        this.workbench = workbench;
    }

    public void run() {
        while (true) {
            try {
                workbench.put();
//                Thread.sleep(new Random().nextInt(1000));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
