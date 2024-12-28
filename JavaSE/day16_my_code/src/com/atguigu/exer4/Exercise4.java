package com.atguigu.exer4;

import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Predicate;

public class Exercise4 {
    public static void main(String[] args) {
        Collection students = new ArrayList();
        students.add(new Student(1, "一", 18));
        students.add(new Student(2, "二", 10));
        students.add(new Student(3, "三", 8));
        for (Object student : students) {
            System.out.println(student);
        }

        System.out.println("==============================");
        students.remove(new Student(1, "一", 18));
        for (Object student : students) {
            System.out.println(student);
        }

        System.out.println("==============================");
        Predicate p = new Predicate() {
            @Override
            public boolean test(Object o) {
                Student s = (Student) o;
                return s.getAge() == 18;
            }
        };
        students.removeIf(p);
        for (Object student : students) {
            System.out.println(student);
        }
    }
}
