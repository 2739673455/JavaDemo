package com.atguigu.homework3;

public class MyArray {
    public static <T> void method(T[] arr, int a, int b) {
        T temp = arr[a];
        arr[a] = arr[b];
        arr[b] = temp;
    }
}
