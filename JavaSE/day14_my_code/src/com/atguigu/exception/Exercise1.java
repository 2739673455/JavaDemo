package com.atguigu.exception;

import java.util.Scanner;

public class Exercise1 {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        int num1, num2;
        while (true) {
            try {
                System.out.print("请输入被除数：");
                num1 = input.nextInt();
                break;
            } catch (Exception e) {
                input.nextLine();
            }
        }

        while (true) {
            try {
                System.out.print("请输入除数：");
                num2 = input.nextInt();
                if (num2 == 0) {
                    System.out.println("除数不能为0");
                } else {
                    break;
                }
            } catch (Exception e) {
                input.nextLine();
            }
        }
        System.out.println(num1 / num2);
        input.close();
    }
}
