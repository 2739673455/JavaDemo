package com.atguigu.exer2;

import java.text.Collator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Locale;

public class Demo {
    public static void main(String[] args) {
        Employee[] emps = new Employee[4];
        emps[0] = new Employee(1, "张三", 2, 18);
        emps[1] = new Employee(2, "李三", 4, 15);
        emps[2] = new Employee(3, "王三", 7, 11);
        emps[3] = new Employee(4, "赵三", 4, 15);
        for (Employee emp : emps) {
            System.out.println(emp);
        }

        Arrays.sort(emps);
        System.out.println("===============================");
        for (Employee emp : emps) {
            System.out.println(emp);
        }

        Comparator<Employee> comparator = new Comparator<>() {
            @Override
            public int compare(Employee o1, Employee o2) {
                int flag = o1.getAge() - o2.getAge();
                if (flag == 0) {
                    Collator c = Collator.getInstance(Locale.CHINA);
                    flag = c.compare(o1.getName(), o2.getName());
                }
                if (flag == 0) {
                    flag = o1.getId() - o2.getId();
                }
                return flag;
            }
        };
        Arrays.sort(emps, comparator);
        System.out.println("===============================");
        for (Employee emp : emps) {
            System.out.println(emp);
        }
    }
}
