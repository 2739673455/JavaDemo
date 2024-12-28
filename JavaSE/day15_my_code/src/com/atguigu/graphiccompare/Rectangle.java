package com.atguigu.graphiccompare;

public class Rectangle implements Comparable<Rectangle> {
    private double length, width;

    public Rectangle() {
    }

    public Rectangle(double length, double width) {
        this.length = length;
        this.width = width;
    }

    public double getWidth() {
        return width;
    }

    public void setWidth(double width) {
        this.width = width;
    }

    public double getLength() {
        return length;
    }

    public void setLength(double length) {
        this.length = length;
    }

    public double area() {
        return length * width;
    }

    public double perimeter() {
        return 2 * (length + width);
    }

    @Override
    public int compareTo(Rectangle o) {
        return -Double.compare(this.length, o.length);
    }

    @Override
    public String toString() {
        return "length: " + length + ", width: " + width + ", area：" + area();
    }
}
