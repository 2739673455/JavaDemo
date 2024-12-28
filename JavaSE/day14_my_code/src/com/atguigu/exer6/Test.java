package com.atguigu.exer6;

public class Test {
    public static void main(String[] args) throws CloneNotSupportedException {
        Rectangle r = new Rectangle(9, 9);
        System.out.println(r);
        try {
            Rectangle r1 = r.clone();
        } catch (CloneNotSupportedException e) {
            Rectangle r1 = new Rectangle(r.getLength(), r.getWidth());
            e.printStackTrace();
        }
    }
}
