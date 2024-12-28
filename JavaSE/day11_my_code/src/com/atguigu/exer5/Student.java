package com.atguigu.exer5;

public class Student extends Person {
    private int score;

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public Student() {
    }

    public Student(String name, int age, char gender, int score) {
        super(name, age, gender);
        this.score = score;
    }

    public String toString() {
        return super.toString() + " 成绩:" + score;
    }
}
