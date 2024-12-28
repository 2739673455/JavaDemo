package com.atguigu.homework4;

import java.util.Arrays;
import java.util.Random;

public class Homework4 {
    public static void main(String[] args) {
        int[] a = new int[10];
        final int length = 6;
        Random random = new Random();
        for (int i = 0; i < a.length - 1; i++) {
            a[i] = random.nextInt(1, 5);
        }
        a[a.length - 1] = 0;
        System.out.println(Arrays.toString(a));

        int max = 1;
        for (int i = 0; i < a.length; i++) {
            int currentLength = 0;
            int count = 1;
            for (int j = i; j < a.length; j++) {
                currentLength += a[j];
                if (currentLength > length) {
                    break;
                } else {
                    count++;
                }
            }
            max = Math.max(max, count);
            System.out.print(count + "，" + currentLength + "\n");
        }
        System.out.println(max);


//        for (int i = 0; i < a.length; i++) {
//            int count = 0;
//            int sum = 0;
//            if (max < a.length - i) {
//                for (int j = i; j < a.length; j++) {
//                    sum += a[j];
//                    if (sum <= length) {
//                        count++;
//                    } else {
//                        break;
//                    }
//                }
//                if (count > max) {
//                    max = count;
//                }
//            }
//        }
//        System.out.println("最多能覆盖" + max + "个点");
    }
}
