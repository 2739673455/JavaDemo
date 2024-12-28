package com.atguigu.homework3;

public enum Status {
    FREE("空闲"),
    USED("在用"),
    SCRAP("报废");

    Status(String description) {
        this.description = description;
        this.value = ordinal();
    }

    private final String description;
    private final int value;

    public static Status getValue(int value) {
        return Status.values()[value];
    }


    @Override
    public String toString() {
        return description;
    }
}
