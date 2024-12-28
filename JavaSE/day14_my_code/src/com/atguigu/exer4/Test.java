package com.atguigu.exer4;

public class Test {
    public static void main(String[] args) {
        int[] arr1 = null;
        int[] arr2 = {};
        int[] arr3 = {3, 2, 1, 4};
        int[] arr4 = {0, 1, 16, 28, 39};
        int target = 1;

        for (int[] arr : new int[][]{arr1, arr2, arr3, arr4}) {
            try {
                System.out.println(arr.toString() + "的最大值为：" + ArrayTools.max(arr));
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                System.out.println(arr.toString() + "的" + target + "在：" + ArrayTools.binarySearch(arr, target));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
