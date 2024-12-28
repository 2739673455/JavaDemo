package com.atguigu.exer4;

public class ArrayTools {
    public static int max(int[] arr) throws ArrayHasNoneElementException {
        if (arr == null) {
            throw new NullPointerException("空数组");
        } else if (arr.length == 0) {
            throw new ArrayHasNoneElementException("数组无元素");
        } else {
            int max = arr[0];
            for (int i = 1; i < arr.length; i++) {
                if (max < arr[i]) {
                    max = arr[i];
                }
            }
            return max;
        }
    }

    public static boolean isSorted(int[] arr) {
        for (int i = 0; i < arr.length - 1; i++) {
            if (arr[i] > arr[i + 1]) {
                return false;
            }
        }
        return true;
    }

    public static int binarySearch(int[] arr, int value) throws ArrayHasNoneElementException, ArrayHasNotSortedException {
        if (arr == null) {
            throw new NullPointerException("空数组");
        } else if (arr.length == 0) {
            throw new ArrayHasNoneElementException("数组无元素");
        } else if (!isSorted(arr)) {
            throw new ArrayHasNotSortedException("数组未排序");
        } else {
            int left = 0;
            int right = arr.length - 1;
            while (left <= right) {
                int mid = left + (right - left) / 2;
                if (arr[mid] == value) {
                    return mid;
                } else if (arr[left] == value) {
                    return left;
                } else if (arr[right] == value) {
                    return right;
                } else if (arr[mid] > value) {
                    right = mid - 1;
                    left++;
                } else {
                    left = mid + 1;
                    right--;
                }
            }
        }
        return -1;
    }
}


