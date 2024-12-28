package com.atguigu.homework6;

import java.util.Objects;

public class Homework6 {
    public static void main(String[] args) {
        String[] arr = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"};
        System.out.println(binarySearch(arr, "e"));
    }

    public static int binarySearch(String[] arr, String target) {
        int left = 0;
        int right = arr.length - 1;
        while (left <= right) {
            int mid = left + (right - left) / 2;
            if (Objects.equals(arr[mid], target)) {
                return mid;
            } else if (target.compareTo(arr[mid]) < 0) {
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }
        return -1;
    }
}
