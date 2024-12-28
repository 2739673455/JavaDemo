package com.atguigu.exer8;

import java.util.Scanner;

public class Demo {
    public static void main(String[] args) {
        Employee[] arrEmployee = new Employee[3];
        arrEmployee[0] = new HourEmployee("张三", 1, 1000);
        arrEmployee[1] = new SalaryEmployee("张四", 100, 1900, 1, 1);
        arrEmployee[2] = new Manager("张五", 200, 1800, 1, 1, 0.2);

        System.out.println("财务请准备资金" + getTotalSalary(arrEmployee));
        Scanner input = new Scanner(System.in);
        System.out.print("输入月份：");
        int month = input.nextInt();
        System.out.println(getBirthdayNoticeList(arrEmployee, month) + "本月生日，领取礼物");
        input.close();
    }

    public static double getTotalSalary(Employee[] arrEmployee) {
        double totalSalary = 0;
        for (int i = 0; i < arrEmployee.length; i++) {
            System.out.println(arrEmployee[i]);
            totalSalary += arrEmployee[i].earning();
        }
        return totalSalary;
    }

    public static String getBirthdayNoticeList(Employee[] arrEmployee, int month) {
        String noticeList = "";
        for (int i = 0; i < arrEmployee.length; i++) {
            if (!(arrEmployee[i] instanceof HourEmployee)) {
                if (((SalaryEmployee) arrEmployee[i]).getBirthday().getMonth() == month) {
                    noticeList += arrEmployee[i].getName() + " ";
                }
            }
        }
        return !noticeList.isEmpty() ? noticeList : "无人";
    }
}
