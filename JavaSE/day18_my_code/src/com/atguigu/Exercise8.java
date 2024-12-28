package com.atguigu;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

public class Exercise8 {
    public static void main(String[] args) {
        HashMap<Integer, ArrayList<String>> map = new HashMap<>();
        map.put(1, new ArrayList<>(Arrays.asList(new String[]{"张一", "李二", "王三"})));
        map.put(2, new ArrayList<>(Arrays.asList(new String[]{"张二", "李三", "王四"})));
        map.put(3, new ArrayList<>(Arrays.asList(new String[]{"张三", "李四", "王五"})));

        Set<Integer> key = map.keySet();
        for (Integer i : key) {
            System.out.println(map.get(i));
        }

        String name = "张三";
        boolean flag = false;
        for (Integer i : key) {
            if (map.get(i).contains(name)) {
                flag = true;
                break;
            }
        }
        System.out.println(flag);
    }
}

