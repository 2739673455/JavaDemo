package com.atguigu.homework4;

import java.util.ArrayList;

public class homework4 {
    public static void main(String[] args) {
        ArrayList<Employee> emps = new ArrayList<>();
        emps.add(new Employee(1, "张一", 9.9, 9, "男"));
        emps.add(new Employee(2, "张二", 19.9, 19, "女"));
        emps.add(new Employee(3, "张三", 29.9, 29, "男"));
        emps.add(new Employee(4, "张四", 39.9, 39, "女"));
        emps.add(new Employee(5, "王五", 49.9, 49, "男"));

//        test1(emps);
//        test2(emps);
//        test3(emps);
//        test4(emps);
//        test5(emps);
        test6(emps);
    }

    public static void test1(ArrayList<Employee> emps) {
        emps.stream().filter((e) -> e.getId() % 2 == 0).forEach(System.out::println);
    }

    public static void test2(ArrayList<Employee> emps) {
        emps.stream().filter((e) -> e.getSalary() < 10000.0).forEach(System.out::println);
    }

    public static void test3(ArrayList<Employee> emps) {
        emps.stream().filter((e) -> e.getAge() > 30 && "女".equals(e.getGender())).forEach(System.out::println);
    }

    public static void test4(ArrayList<Employee> emps) {
        emps.stream().filter((e) -> e.getName().contains("张")).forEach(System.out::println);
    }

    public static void test5(ArrayList<Employee> emps) {
        emps.removeIf((e) -> e.getAge() > 30 && "女".equals(e.getGender()));
        emps.removeIf((e) -> "张三".equals(e.getName()));
        emps.forEach(e -> e.setSalary(e.getSalary() * 1.1));
        emps.forEach(System.out::println);
    }

    public static void test6(ArrayList<Employee> emps) {
        emps.stream().map(e -> {
            e.setSalary(e.getSalary() * 1.1);
            return e;
        }).forEach(System.out::println);
    }
}
