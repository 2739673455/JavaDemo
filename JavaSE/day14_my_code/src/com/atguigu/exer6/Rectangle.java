package com.atguigu.exer6;

public class Rectangle implements Cloneable {
    private double length, width;

    public Rectangle() {
    }

    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }

    public double getWidth() {
        return width;
    }

    public void setWidth(double width) {
        this.width = width;
    }

    public Rectangle(double length, double width) {
        this.length = length;
        this.width = width;
    }

    public String toString() {
        return "length=" + length + ", width=" + width;
    }

    @Override
    public Rectangle clone() throws CloneNotSupportedException {
        return (Rectangle) (super.clone());
    }
}
