package com.atguigu.exer2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Scanner;

public class Exercise2 {
    public static void main(String[] args) {
        Collection employees = new ArrayList();
        employees.add(new Employee(1, "一", 10.0));
        employees.add(new Employee(2, "二", 11.1));
        employees.add(new Employee(3, "三", 12.2));
        employees.add(new Employee(4, "四", 13.3));
        employees.add(new Employee(5, "五", 15001));

        Object[] all = employees.toArray();
        for (Object employee : all) {
            System.out.println(employee);
        }

        Scanner input = new Scanner(System.in);
        System.out.print("输入员工名称：");
        String empName = input.next();
        boolean flag = false;
        for (Object employee : all) {
            if (((Employee) employee).getName().equals(empName)) {
                flag = true;
                break;
            }
        }
        if (flag) {
            System.out.println((empName + "在其中"));
        } else {
            System.out.println((empName + "不在其中"));
        }
        input.close();


        Iterator iterator = employees.iterator();
        while (iterator.hasNext()) {
            if (((Employee) iterator.next()).getSalary() > 15000.0) {
                iterator.remove();
            }
        }
        all = employees.toArray();
        for (Object employee : all) {
            System.out.println(employee);
        }
    }

}
