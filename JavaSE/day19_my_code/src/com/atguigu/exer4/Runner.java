package com.atguigu.exer4;

public class Runner extends Thread {
    private static int totalDistance;
    private String name;
    private int speed;
    private int sleepTime;
    private int sleepDistance;
    private int currentDistance;
    private int currentSleepTime;
    private int currentSleepDistance;
    private long elapsedTime;

    public Runner(String name, int speed, int sleepTime, int sleepDistance) {
        this.name = name;
        this.speed = speed;
        this.sleepTime = sleepTime;
        this.sleepDistance = sleepDistance;
        this.currentDistance = 0;
        this.currentSleepTime = sleepTime;
        this.currentSleepDistance = sleepDistance;

        setName(name);
    }

    public void run() {
        long startTime = System.currentTimeMillis();
        boolean running = true;
        while (currentDistance < totalDistance) {
            if (running) {
                currentDistance += speed;
                currentSleepDistance -= speed;
                System.out.println(name + "跑了" + speed + "米");
                if (currentSleepDistance == 0) {
                    running = false;
                    currentSleepDistance = sleepDistance;
                }
            } else {
                --currentSleepTime;
                System.out.println(name + "睡了" + (sleepTime - currentSleepTime) + "秒");
                if (currentSleepTime == 0) {
                    running = true;
                    currentSleepTime = sleepTime;
                }
            }
            if (!rest() || currentDistance >= totalDistance) {
                break;
            }
        }
        long endTime = System.currentTimeMillis();
        elapsedTime = endTime - startTime;
    }

    public long getElapsedTime() {
        return elapsedTime;
    }

    public static int getTotalDistance() {
        return totalDistance;
    }

    public static void setTotalDistance(int totalDistance) {
        Runner.totalDistance = totalDistance;
    }

    public int getCurrentDistance() {
        return currentDistance;
    }

    private boolean rest() {
        try {
            Thread.sleep(1000);
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
}
