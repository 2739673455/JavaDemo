package com.atguigu.exer1light;

public class TurnSignal extends Lamp {
    @Override
    public void on() {
        System.out.println("转向灯打开，闪烁亮");
    }

    @Override
    public void off() {
        System.out.println("转向灯关闭");
    }
}
