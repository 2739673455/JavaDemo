package com.atguigu.generic;

import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeSet;

public class Demo {
    public static void main(String[] args) {
        HashSet<Employee> employees = new HashSet<>();
        employees.add(new Employee("张三", 10));
        employees.add(new Employee("张三", 10));
        employees.add(new Employee("李四", 11));
        System.out.println(employees);

        Comparator<Employee> comparator = new Comparator<>() {
            @Override
            public int compare(Employee o1, Employee o2) {
                return -o1.getName().compareTo(o2.getName());
            }
        };

        TreeSet<Employee> treeSet = new TreeSet<>(comparator);
        treeSet.add(new Employee("张三", 10));
        treeSet.add(new Employee("张三", 10));
        treeSet.add(new Employee("李四", 11));
        System.out.println(employees);
    }
}
