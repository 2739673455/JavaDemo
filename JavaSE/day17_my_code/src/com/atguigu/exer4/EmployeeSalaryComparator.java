package com.atguigu.exer4;

import java.util.Comparator;

public class EmployeeSalaryComparator implements Comparator<Employee> {
    @Override
    public int compare(Employee e1, Employee e2) {
        int result = Double.compare(e1.getSalary(), e2.getSalary());
        if (result == 0) {
            result = e1.getId() - e2.getId();
        }
        return result;
    }
}
