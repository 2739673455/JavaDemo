package com.atguigu.homework3;

public class Architect extends Designer {
    private int stock;

    public Architect() {
    }

    public Architect(String id, String name, int age, double salary, String job, double bonus, int stock) {
        super(id, name, age, salary, job, bonus);
        this.stock = stock;
    }

    public int getStock() {
        return stock;
    }

    public void setStock(int stock) {
        this.stock = stock;
    }

    public String toString() {
        return super.toString() + "\t" + stock;
    }
}
