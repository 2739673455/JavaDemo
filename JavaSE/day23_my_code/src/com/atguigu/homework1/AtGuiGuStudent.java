package com.atguigu.homework1;

import java.io.Serializable;

public class AtGuiGuStudent implements Serializable, Comparable<AtGuiGuStudent> {
    private static final long servialVersionUID = 20240625L;
    private static String school;
    private String className;

    public AtGuiGuStudent(String className) {
        this.className = className;
    }

    public static String getSchool() {
        return school;
    }

    public static void setSchool(String school) {
        AtGuiGuStudent.school = school;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public AtGuiGuStudent() {
    }

    @Override
    public int compareTo(AtGuiGuStudent o) {
        return this.className.compareTo(o.className);
    }

    @Override
    public String toString() {
        return "AtGuiGuStudent{" +
                "className='" + className + '\'' +
                '}';
    }
}
