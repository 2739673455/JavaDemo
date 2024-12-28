package com.atguigu.homework8;

import java.util.Arrays;

public class Homework8 {
    public static void main(String[] args) {
        int[] ii = {1, 3, 4, 8, 9, 10, 5};
        System.out.println(Arrays.toString(changePosition(5, 1, ii)));

    }

    public static int[] changePosition(int n1, int n2, int[] ii) {
        int i = 0;
        int j = 0;
        int[] arr = new int[ii.length];
        while (i < ii.length && j < ii.length) {
            if (ii[i] == n1) {
                i++;
            } else if (ii[i] == n2) {
                arr[j++] = ii[i++];
                arr[j++] = n1;
            } else {
                arr[j++] = ii[i++];
            }
        }
        return arr;
    }
}
