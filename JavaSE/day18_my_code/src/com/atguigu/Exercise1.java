package com.atguigu;

import java.util.*;

public class Exercise1 {
    public static void main(String[] args) {
        List<Integer> list = new ArrayList<Integer>();
        Random random = new Random();
        for (int i = 0; i < 10; ++i) {
            while (true) {
                int num = random.nextInt(20);
                if (num % 2 == 0 && num != 0) {
                    list.add(num);
                    break;
                }
            }
        }

        System.out.println(list.size());
        for (Integer num : list) {
            System.out.print(num + " ");
        }

        System.out.println();
        for (ListIterator<Integer> iterator = list.listIterator(list.size()); iterator.hasPrevious(); ) {
            System.out.print(iterator.previous() + " ");
        }

        int index = 0;
        boolean sign = false;
        for (Iterator<Integer> iterator2 = list.iterator(); iterator2.hasNext(); ) {
            int num = iterator2.next();
            if (sign) {
                iterator2.remove();
            } else if (num == 12) {
                sign = true;
            } else {
                ++index;
            }
        }
        
        System.out.println("\n" + index);
        System.out.println(list);
    }
}
