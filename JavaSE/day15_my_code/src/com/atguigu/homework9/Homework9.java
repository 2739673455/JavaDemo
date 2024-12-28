package com.atguigu.homework9;

import java.util.Arrays;

public class Homework9 {
    public static void main(String[] args) {
        int[] arr = new int[1000];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = i;
        }
        System.out.println(loop(arr));
        answer(1000);
    }


    public static void show(int[] arr) {
        System.out.println(Arrays.toString(arr));
    }

    public static int loop(int[] arr) {
        int totalCount = arr.length;
        int currentCount = 2;
        int index = 0;
        while (totalCount > 0) {
            if (arr[index] != -1) {
                currentCount--;
            }
            if (currentCount < 0) {
                arr[index] = -1;
                totalCount--;
                currentCount = 2;
//                System.out.println(Arrays.toString(arr) + " index:" + index + " totalCount:" + totalCount);
            }
            if (totalCount == 0) {
                break;
            }
            index++;
            if (index == arr.length) {
                index = 0;
            }
        }
        return index;
    }

    public static void answer(int length) {
        int[] a = new int[length];
        for (int i = 0; i < a.length; i++) {
            a[i] = i;
        }
        int i = 0;
        int count = 0;
        int delNum = 0;//被删掉的数字个数
        while (true) {
            if (a[i] != -1) {//被删掉的数不再计入count个数
                count++;
            }
            if (count == 3) {//隔两个，第三个删掉
                a[i] = -1;//a[i]=-1，表示被删掉的数
                count = 0;//count重新计数
                delNum++;//统计已经被删掉几个了
//                System.out.println(Arrays.toString(a) + " index:" + i + " totalCount:" + delNum);
            }
            if (delNum == a.length - 1) {//留下最后一个结束删除过程
                break;
            }
            if (++i == a.length) {//如果下标右移已经到头了，要从头开始
                i = 0;
            }
        }

        for (int j = 0; j < a.length; j++) {
            if (a[j] != -1) {
                System.out.println("最后一个被删掉的数是：a[j]=" + a[j] + "，它的下标：" + j);
            }
        }
    }
}
