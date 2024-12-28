package com.atguigu.exer5;

public class Exercise5 {
    public static void main(String[] args) {
        Person p1 = new Person("张三", 18, '男');
        Student s1 = new Student("张四", 19, '男', 99);
        Teacher t1 = new Teacher("张五", 20, '男', 100000000);
        System.out.println(p1);
        System.out.println(s1);
        System.out.println(t1);
    }
}
