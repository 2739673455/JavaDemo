package com.atguigu.homework3;

public class Demo {
    public static void main(String[] args) {
        Employee[] emps = new Employee[5];
        emps[0] = new Employee("1", "张三", 18, 10000);
        emps[1] = new Programmer("2", "张四", 18, 10001, "程序员");
        emps[2] = new Designer("3", "张五", 18, 10002, "设计师", 100);
        emps[3] = new Architect("4", "张六", 18, 10003, "架构师", 100, 200);
        for (Employee emp : emps) {
            if (emp != null) {
                System.out.println(emp);
            }
        }
    }
}
