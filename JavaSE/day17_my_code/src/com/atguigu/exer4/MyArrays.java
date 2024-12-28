package com.atguigu.exer4;

import java.util.Comparator;

public class MyArrays {
    public static <T> int find(T[] arr, T value) {
        for (int i = 0; i < arr.length; ++i) {
            if (arr[i].equals(value)) {
                return i;
            }
        }
        return -1;
    }

    public static <T extends Comparable<? super T>> T max(T[] arr) {
        T max = arr[0];
        for (int i = 0; i < arr.length; ++i) {
            if (arr[i].compareTo(max) > 0) {
                max = arr[i];
            }
        }
        return max;
    }

    public static <T> T max(T[] arr, Comparator<? super T> c) {
        T max = arr[0];
        for (int i = 0; i < arr.length; ++i) {
            if (c.compare(arr[i], max) > 0) {
                max = arr[i];
            }
        }
        return max;
    }
}
