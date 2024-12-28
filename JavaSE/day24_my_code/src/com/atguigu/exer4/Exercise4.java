package com.atguigu.exer4;

import java.util.HashMap;

public class Exercise4 {
    public static void main(String[] args) {
        HashMap<Integer, Employee> emps = new HashMap<>();
        emps.put(1, new Employee(1, "张三", 900000.9));
        emps.put(2, new Employee(2, "张四", 9.9));
        emps.put(3, new Employee(3, "张五", 9.9));

        emps.forEach((k, v) -> System.out.println(k + " -> " + v));
        emps.replaceAll((k, v) -> {
            if (v.getSalary() < 10000.0) {
                v.setSalary(10000.0);
            }
            return v;
        });
        emps.forEach((k, v) -> System.out.println(k + " -> " + v));

    }
}
