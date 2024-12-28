package com.atguigu.exer3liveable;

public interface LiveAble {
    void eat();

    void breathe();

    default void sleep() {
        System.out.println("静止不动");
    }

    static void drink() {
        System.out.println("喝水");
    }
}
