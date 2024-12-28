package com.atguigu.exer5;

import java.util.ArrayList;
import java.util.function.Predicate;

public class EmployeeService {
    private ArrayList<Employee> all = new ArrayList<>();

    public void add(Employee emp) {
        all.add(emp);
    }

    public ArrayList<Employee> get(Predicate<Employee> p) {
        ArrayList<Employee> result = new ArrayList<>();
        all.forEach((emp) -> {
            if (p.test(emp)) {
                result.add(emp);
            }
        });
        return result;
    }
}
