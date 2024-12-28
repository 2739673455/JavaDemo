package com.atguigu.exer4;

public class Demo {
    public static void main(String[] args) throws InterruptedException {
        Runner.setTotalDistance(30);
        Runner rabbit = new Runner("兔子", 10, 10, 10);
        Runner tortoise = new Runner("乌龟", 1, 1, 10);

        rabbit.start();
        tortoise.start();
//        rabbit.join();
//        tortoise.join();


        while (rabbit.isAlive() && tortoise.isAlive()) {
        }
        rabbit.interrupt();
        tortoise.interrupt();
        if (rabbit.getCurrentDistance() == Runner.getTotalDistance()) {
            System.out.println(rabbit.getName() + "获胜，用时" + rabbit.getElapsedTime() / 1000 + "秒");
        } else {
            System.out.println(tortoise.getName() + "获胜，用时" + rabbit.getElapsedTime() / 1000 + "秒");
        }
    }
}
