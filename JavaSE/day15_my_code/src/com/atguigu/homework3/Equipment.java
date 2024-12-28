package com.atguigu.homework3;

public class Equipment {
    private int id;
    private String brand;
    private double price;
    private String name;
    private Status status;

    public Equipment(int id, String brand, double price, String name, Status status) {
        this.id = id;
        this.brand = brand;
        this.price = price;
        this.name = name;
        this.status = status;
    }

    public Equipment() {
    }

    @Override
    public String toString() {
        return "编号：" + id +
                ",品牌：" + brand +
                ",价格：" + price +
                ",名称：" + name +
                ",状态：" + status;
    }
}
