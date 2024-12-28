package com.atguigu;

import java.util.Scanner;
import java.util.TreeSet;

public class Exercise5 {
    public static void main(String[] args) {
        TreeSet<Character> charSet = new TreeSet<>();
        Scanner input = new Scanner(System.in);
        for (int i = 0; i < 10; i++) {
            System.out.print("请输入第" + i + "个单词:");
            char[] charList = input.next().toCharArray();
            for (char c : charList) {
                charSet.add(c);
            }
        }
        input.close();

        TreeSet<Character> charSet2 = new TreeSet<>();
        for (int i = 97; i <= 122; ++i) {
            charSet2.add((char) i);
        }

        System.out.println(charSet);
        charSet2.removeAll(charSet);
        System.out.println(charSet2);
    }
}
