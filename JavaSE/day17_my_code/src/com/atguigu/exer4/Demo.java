package com.atguigu.exer4;

public class Demo {
    public static void main(String[] args) {
        Employee[] arr = new Employee[5];
        arr[0] = new Employee(1, "张三", 18, 19);
        arr[1] = new Employee(2, "李三", 17, 15);
        arr[2] = new Employee(3, "王三", 18, 13);
        arr[3] = new Employee(4, "赵三", 20, 15);
        arr[4] = new Employee(5, "刘三", 16, 19);


        System.out.println(MyArrays.find(arr, new Employee(3, "王三", 18, 13)));
        System.out.println(MyArrays.max(arr));
        System.out.println(MyArrays.max(arr, new EmployeeSalaryComparator()));
        System.out.println(MyArrays.max(arr, new PersonAgeComparator()));
    }
}
