package com.atguigu.signle;

public class SignleTwo {
    private static SignleTwo instance = new SignleTwo();

    private SignleTwo() {
    }

    public static SignleTwo getInstance() {
        return instance;
    }
}
