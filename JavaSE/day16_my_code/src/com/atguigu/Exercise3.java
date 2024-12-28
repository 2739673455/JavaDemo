package com.atguigu;

import java.util.*;

public class Exercise3 {
    public static void main(String[] args) {
        ArrayList primeNumbers = new ArrayList();
        int[] nums = createNumArray(100);
        primeFilter(nums);
        for (int i = 0; i < nums.length; i++) {
            if (nums[i] != 0) {
                primeNumbers.add(nums[i]);
            }
        }
        System.out.println(primeNumbers);

        Iterator iterator = primeNumbers.iterator();
        while (iterator.hasNext()) {
            if ((Integer) (iterator.next()) % 10 == 3) {
                iterator.remove();
            }
        }
        System.out.println(primeNumbers);

        Random random = new Random();
        Collection randNumbers = new ArrayList();
        for (int i = 0; i < 10; i++) {
            randNumbers.add(random.nextInt(100));
        }
        System.out.println(randNumbers);

        primeNumbers.retainAll(randNumbers);
        System.out.println(primeNumbers);
    }

    public static int[] createNumArray(int n) {
        int[] nums = new int[n];
        for (int i = 0; i < n; i++) {
            nums[i] = i;
        }
        return nums;
    }

    public static void primeFilter(int[] nums) {
        nums[1] = 0;
        for (int i = 2; i < nums.length; ++i) {
            if (nums[i] == 0) {
                continue;
            }
            int j = i * i;
            if (j > nums.length) {
                break;
            }
            while (j < nums.length) {
                System.out.println(j);
                nums[j] = 0;
                j += i;
            }
            System.out.println("=================");
        }
        System.out.println(Arrays.toString(nums));
    }

    public static void primeFilter2(int[] nums) {
        nums[1] = 0;
        for (int i = 2; i < 100; i++) {
            for (int j = 2; j <= Math.sqrt(i); j++) {
                if (i % j == 0) {
                    nums[i] = 0;
                    break;
                }
            }
            System.out.println("=================");
        }
    }
}

