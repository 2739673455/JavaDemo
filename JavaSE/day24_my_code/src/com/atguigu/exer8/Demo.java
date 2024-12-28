package com.atguigu.exer8;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Demo {
    public static void main(String[] args) {
        ArrayList<Employee> emps = new ArrayList<>();
        emps.add(new Employee("张三", 9, 99.9));
        emps.add(new Employee("张四", 19, 9999.9));
        emps.add(new Employee("张五", 29, 9999.9));
        emps.add(new Employee("张六", 39, 9.9));
        emps.add(new Employee("张七", 49, 999.9));

        emps.forEach(System.out::println);

        Collections.sort(emps, (o1, o2) -> Double.compare(o1.getSalary(), o2.getSalary()));
        System.out.println("=================================");
        emps.forEach(System.out::println);

        Collections.sort(emps, Comparator.comparingDouble(Employee::getAge));
        System.out.println("=================================");
        emps.forEach(System.out::println);
    }
}
