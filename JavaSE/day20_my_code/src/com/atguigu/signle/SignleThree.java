package com.atguigu.signle;

public class SignleThree {
    private static SignleThree instance;

    private SignleThree() {
    }

    public synchronized static SignleThree getInstance() {
        if (instance == null) {
            instance = new SignleThree();
        }
        return instance;
    }
}
