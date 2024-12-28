package com.atguigu;

import java.util.ArrayList;
import java.util.Random;
import java.util.Scanner;
import java.util.function.Predicate;

public class Exercise1 {
    public static void main(String[] args) {
        Random random = new Random();
        ArrayList randNumbers = new ArrayList();
        for (int i = 0; i < 10; ++i) {
            randNumbers.add(random.nextInt(100));
        }
        System.out.println(randNumbers);

        Scanner input = new Scanner(System.in);
        System.out.print("请输入要删除的数：");
        int removeNumber = input.nextInt();
        input.close();

        randNumbers.remove(Integer.valueOf(removeNumber));
        System.out.println(randNumbers);

        Predicate p = new Predicate() {
            @Override
            public boolean test(Object o) {
                Integer i = (Integer) o;
                if (i % 10 == 3) {
                    System.out.println("删除:" + i);
                    return true;
                }
                return false;
            }
        };
        randNumbers.removeIf(p);
        System.out.println(randNumbers);
    }
}
