package com.atguigu.homework10;

import java.util.function.Predicate;

public class EmployeeService {
    private Employee[] arr = new Employee[5];
    private int total;

    public Employee[] getEmployees() {
        return arr;
    }

    public void add(Employee emp) {
        arr[total++] = emp;
    }

    public Employee[] get(Predicate p) {
        int count = 0;
        boolean[] flag = new boolean[total];
        Employee[] result;
        for (int i = 0; i < total; i++) {
            if (p.test(arr[i])) {
                count++;
                flag[i] = true;
            }
        }
        int j = 0;
        result = new Employee[count];
        for (int i = 0; i < total; i++) {
            if (flag[i]) {
                result[j] = arr[i];
                j++;
            }
        }
        return result;
    }
}
