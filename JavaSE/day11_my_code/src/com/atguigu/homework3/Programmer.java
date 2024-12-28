package com.atguigu.homework3;

public class Programmer extends Employee {
    private String job;

    public Programmer() {
    }

    public Programmer(String id, String name, int age, double salary, String job) {
        super(id, name, age, salary);
        this.job = job;
    }

    public String getJob() {
        return job;
    }

    public void setJob(String job) {
        this.job = job;
    }

    @Override
    public String toString() {
        return super.toString() + "\t" + job;
    }
}
