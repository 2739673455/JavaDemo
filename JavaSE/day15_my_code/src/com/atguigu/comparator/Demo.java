package com.atguigu.comparator;

import java.util.Arrays;
import java.util.Comparator;

public class Demo {
    public static void main(String[] args) {
        Employee[] all = new Employee[5];
        all[0] = new Employee(1, "张三", 24, 14000, 130);
        all[1] = new Employee(2, "李四", 24, 18000, 120);
        all[2] = new Employee(3, "熊大", 26, 13000, 220);
        all[3] = new Employee(4, "熊二", 22, 16000, 200);
        all[4] = new Employee(5, "光头强", 23, 11000, 110);

        print(all);


        Comparator idComparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                Employee e1 = (Employee) o1;
                Employee e2 = (Employee) o2;
                return Integer.compare(e1.getId(), e2.getId());
            }
        };

        Comparator ageComparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                Employee e1 = (Employee) o1;
                Employee e2 = (Employee) o2;
                return Integer.compare(e1.getAge(), e2.getAge());
            }
        };

        Comparator salaryComparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                Employee e1 = (Employee) o1;
                Employee e2 = (Employee) o2;
                return Double.compare(e1.getSalary(), e2.getSalary());
            }
        };

        Comparator weightComparator = new Comparator() {
            @Override
            public int compare(Object o1, Object o2) {
                Employee e1 = (Employee) o1;
                Employee e2 = (Employee) o2;
                return Double.compare(e1.getWeight(), e2.getWeight());
            }
        };

        Arrays.sort(all, idComparator);
        System.out.println("按ID排序=====================");
        print(all);

        Arrays.sort(all, ageComparator);
        System.out.println("按年龄排序=====================");
        print(all);

        Arrays.sort(all, salaryComparator);
        System.out.println("按薪资排序=====================");
        print(all);

        Arrays.sort(all, weightComparator);
        System.out.println("按重量排序=====================");
        print(all);
    }

    public static void print(Employee[] all) {
        for (Employee e : all) {
            System.out.println(e);
        }
    }
}
