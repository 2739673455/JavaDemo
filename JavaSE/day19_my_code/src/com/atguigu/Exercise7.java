package com.atguigu;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Set;

public class Exercise7 {
    public static void main(String[] args) {
        HashMap<Integer, String> map = new HashMap<>();
        map.put(1930, "乌拉圭");
        map.put(1934, "意大利");
        map.put(1938, "意大利");
        map.put(1950, "乌拉圭");
        map.put(1954, "西德");
        map.put(1958, "巴西");
        map.put(1962, "巴西");
        map.put(1966, "英格兰");
        map.put(1970, "巴西");
        map.put(1974, "西德");
        map.put(1978, "阿根廷");
        map.put(1982, "意大利");
        map.put(1986, "阿根廷");
        map.put(1990, "西德");
        map.put(1994, "巴西");
        map.put(1998, "法国");
        map.put(2002, "巴西");
        map.put(2006, "意大利");
        map.put(2010, "西班牙");
        map.put(2014, "德国");
        map.put(2018, "法国");

        findWinner(map, 1982);
        findYear(map, "巴西");
        findYear(map, "荷兰");
    }

    public static void findWinner(HashMap<Integer, String> map, int year) {
        String winner = map.get(year);
        if (winner == null) {
            System.out.println(year + "没有举办世界杯");
        } else {
            System.out.println(year + "冠军是" + winner);
        }
    }

    public static void findYear(HashMap<Integer, String> map, String winner) {
        ArrayList<Integer> years = new ArrayList<>();
        Set<Integer> yearSet = map.keySet();
        for (Integer year : yearSet) {
            if (map.get(year).equals(winner)) {
                years.add(year);
            }
        }
        if (years.size() == 0) {
            System.out.println(winner + "没有获得过冠军");
        } else {
            System.out.println(winner + "球队在" + years + "获得过冠军");
        }
    }
}
