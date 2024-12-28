package com.atguigu.exer1light;

public class Demo {
    public static void main(String[] args) {
        Lamp lamp = new Lamp();
        TurnSignal turnSignal = new TurnSignal();
        lamp.on();
        lamp.off();
        turnSignal.on();
        turnSignal.off();
    }
}
