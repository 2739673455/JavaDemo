package com.atguigu.exception;

import java.util.Scanner;

public class TestTryCatch2 {
    public static void main(String[] args) {
        Scanner input = new Scanner(System.in);
        System.out.print("请输入第一个字符串：");
        String str1 = input.next();
        System.out.print("请输入第二个字符串：");
        String str2 = input.next();
        try {
            double num1 = Double.parseDouble(str1);
            double num2 = Double.parseDouble(str2);
            double result = num1 / num2;
            System.out.println(result);
        } catch (Exception e) {
            String result = str1 + str2;
            System.out.println(result);
        }
        input.close();
    }
}
