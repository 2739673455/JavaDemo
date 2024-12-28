package com.atguigu.homework10;

import java.util.function.Predicate;

public class Test {
    public static void main(String[] args) {
        EmployeeService empS = new EmployeeService();
        empS.add(new Employee(3, "张三", 23, 13000));
        empS.add(new Employee(4, "李四", 24, 24000));
        empS.add(new Employee(5, "王五", 25, 15000));
        empS.add(new Employee(1, "赵六", 27, 17000));
        empS.add(new Employee(2, "钱七", 16, 6000));

        System.out.println("所有员工：");
        Predicate p;
        p = new Predicate() {
            @Override
            public boolean test(Object o) {
                return true;
            }
        };
        ArrayTools.print(empS.get(p));

        System.out.println("所有年龄超过25的员工：");
        p = new Predicate() {
            @Override
            public boolean test(Object o) {
                return ((Employee) o).getAge() > 25;
            }
        };
        ArrayTools.print(empS.get(p));

        System.out.println("所有薪资超过15000的员工：");
        p = new Predicate() {
            @Override
            public boolean test(Object o) {
                return ((Employee) o).getSalary() > 15000;
            }
        };
        ArrayTools.print(empS.get(p));

        System.out.println("所有年龄超过25且薪资高于15000的员工：");
        p = new Predicate() {
            @Override
            public boolean test(Object o) {
                return ((Employee) o).getAge() > 25 && ((Employee) o).getSalary() > 15000;
            }
        };
        ArrayTools.print(empS.get(p));
    }
}
