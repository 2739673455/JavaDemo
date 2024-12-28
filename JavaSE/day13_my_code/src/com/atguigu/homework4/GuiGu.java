package com.atguigu.homework4;

public class GuiGu {
    public static void main(String[] args) {
        new Object() {
            public void print() {
                System.out.println("尚硅谷");
            }
        }.print();
    }
}
