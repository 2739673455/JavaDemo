package com.atguigu.exer4;

import java.util.Comparator;

public class PersonAgeComparator implements Comparator<Person> {
    @Override
    public int compare(Person p1, Person p2) {
        int result = p1.getAge() - p2.getAge();
        if (result == 0) {
            result = p1.getId() - p2.getId();
        }
        return result;
    }
}
