package com.atguigu.exer1light;

public class Lamp extends Light {
    @Override
    public void on() {
        System.out.println("台灯打开，灯常亮");
    }

    @Override
    public void off() {
        System.out.println("台灯关闭");
    }
}
