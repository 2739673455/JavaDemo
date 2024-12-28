package com.atguigu;

import java.util.HashMap;
import java.util.Scanner;

public class Exercise7 {
    public static void main(String[] args) {
        HashMap<String, String> map = new HashMap<>();
        Scanner input = new Scanner(System.in);
        for (int i = 0; i < 3; i++) {
            System.out.println("请输入姓名与手机号码:");
            map.put(input.next(), input.next());
        }
        System.out.println(map);
        System.out.println(map.get(input.next()));
        input.close();

    }
}
