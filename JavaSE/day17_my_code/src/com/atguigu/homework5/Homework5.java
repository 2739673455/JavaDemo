package com.atguigu.homework5;

import java.util.Comparator;

public class Homework5 {
    public static void main(String[] args) {
        Student[] students = new Student[4];
        students[0] = new Student("liusan", 20, 90);
        students[1] = new Student("lisi", 22, 90);
        students[2] = new Student("wangwu", 20, 99);
        students[3] = new Student("sunliu", 22, 100);
        for (Student student : students) {
            System.out.println(student);
        }

        System.out.println("=====Comparable比较器=====");
        MyArray.sort(students);
        for (Student student : students) {
            System.out.println(student);
        }


        System.out.println("=====Comparator比较器=====");
        MyArray.sort(students, new Comparator<Student>() {
            @Override
            public int compare(Student o1, Student o2) {
                return o1.getName().compareTo(o2.getName());
            }
        });
        for (Student student : students) {
            System.out.println(student);
        }
    }

}
