package com.atguigu.signle;

public class SignleFour {

    private class Inner {
        static SignleFour instance = new SignleFour();
    }

    public static SignleFour getInstance() {
        return Inner.instance;
    }
}
