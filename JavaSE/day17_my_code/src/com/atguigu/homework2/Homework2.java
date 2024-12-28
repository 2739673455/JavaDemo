package com.atguigu.homework2;

import java.util.ArrayList;
import java.util.Scanner;

public class Homework2 {
    public static void main(String[] args) {
        ArrayList<Student> students = new ArrayList<>();
        Scanner input = new Scanner(System.in);

        int flag = 1;
        while (flag == 1) {
            System.out.print("请输入学生姓名，年龄：");
            students.add(new Student(input.next(), input.nextInt()));
            System.out.print("1:继续录入，0:结束录入 ：");
            flag = input.nextInt();
        }
        input.close();

        for (Student student : students) {
            System.out.println(student);
        }

        System.out.println("=====年龄大于25=====");
        students.removeIf(student -> student.getAge() < 25);
        for (Student student : students) {
            System.out.println(student);
        }
    }
}
