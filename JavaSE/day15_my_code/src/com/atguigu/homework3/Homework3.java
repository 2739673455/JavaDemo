package com.atguigu.homework3;

public class Homework3 {
    public static void main(String[] args) {
        Equipment[] e = new Equipment[10];
        for (int i = 0; i < 10; i++) {
            e[i] = new Equipment(Integer.parseInt(Data.EQUIPMENTS[i][0]),
                    Data.EQUIPMENTS[i][1],
                    Double.parseDouble(Data.EQUIPMENTS[i][2]),
                    Data.EQUIPMENTS[i][3],
                    Status.getValue(Integer.parseInt(Data.EQUIPMENTS[i][4])));
        }

        for (int i = 0; i < 10; i++) {
            System.out.println(e[i]);
        }
    }
}
