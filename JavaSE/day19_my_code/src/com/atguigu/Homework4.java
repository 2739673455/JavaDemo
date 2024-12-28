package com.atguigu;

import java.util.*;

public class Homework4 {
    public static void main(String[] args) {
        String str = "Your future depends on your dreams, so go to sleep.";

        TreeMap<String, Integer> charMap = new TreeMap<>();
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) != ' ' && str.charAt(i) != ',' && str.charAt(i) != '.') {
                if (charMap.get(String.valueOf(str.charAt(i))) == null) {
                    charMap.put(String.valueOf(str.charAt(i)), 1);
                } else {
                    charMap.put(String.valueOf(str.charAt(i)), charMap.get(String.valueOf(str.charAt(i))) + 1);
                }
            }
        }

//        System.out.println(answer1(charMap));
        System.out.println(answer2(charMap));
    }

    public static List<Map.Entry<String, Integer>> answer1(TreeMap<String, Integer> charMap) {
        List<Map.Entry<String, Integer>> list = new ArrayList<>(charMap.entrySet());
        list.sort(Map.Entry.comparingByValue());
        return list;
    }

    public static Map<String, Integer> answer2(TreeMap<String, Integer> charMap) {
        Map<String, Integer> result = new TreeMap<>(new ValueComparator(charMap));
        result.putAll(charMap);
        return result;
    }
}

class ValueComparator implements Comparator<String> {
    Map<String, Integer> map;

    public ValueComparator() {
    }

    public ValueComparator(Map<String, Integer> map) {
        this.map = map;
    }

    public int compare(String o1, String o2) {
        if (Objects.equals(map.get(o2), map.get(o1)))
            return 1;
        else
            return -(Integer.compare(map.get(o1), map.get(o2)));
    }
}