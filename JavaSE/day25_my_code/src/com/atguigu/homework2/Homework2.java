package com.atguigu.homework2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Homework2 {
    public static void main(String[] args) {
        ArrayList<Student> students = new ArrayList<>();
        students.add(new Student("谢霆锋", 85));
        students.add(new Student("章子怡", 63));
        students.add(new Student("刘亦菲", 77));
        students.add(new Student("黄晓明", 33));
        students.add(new Student("岑小村", 92));

//        sorted1(students);
//        sorted2(students);
        sorted3(students);
    }


    public static void sorted1(ArrayList<Student> students) {
        Collections.sort(students, new Comparator<Student>() {
            @Override
            public int compare(Student o1, Student o2) {
                return o1.getScore() - o2.getScore();
            }
        });
        students.forEach(System.out::println);
    }

    public static void sorted2(ArrayList<Student> students) {
        Collections.sort(students, (o1, o2) -> o1.getScore() - o2.getScore());
        students.forEach(System.out::println);

    }

    public static void sorted3(ArrayList<Student> students) {
        Collections.sort(students, Comparator.comparingInt(Student::getScore));
        students.forEach(System.out::println);

    }
}
