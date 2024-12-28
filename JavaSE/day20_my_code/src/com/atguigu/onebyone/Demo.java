package com.atguigu.onebyone;

public class Demo {
    public static void main(String[] args) throws InterruptedException {
        Workbench workbench = new Workbench();
        Puter puter1 = new Puter("厨师1", workbench);
        Taker taker1 = new Taker("服务员1", workbench);

        puter1.start();
        taker1.start();
    }
}
