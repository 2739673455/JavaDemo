package com.atguigu.homework5;

import java.util.stream.Stream;

public class Homework5 {
    public static void main(String[] args) {
        String[] provinces = {"河北省",
                "山西省",
                "吉林省",
                "辽宁省",
                "黑龙江省",
                "陕西省",
                "甘肃省",
                "青海省",
                "山东省",
                "福建省",
                "浙江省",
                "台湾省",
                "河南省",
                "湖北省",
                "湖南省",
                "江西省",
                "江苏省",
                "安徽省",
                "广东省",
                "海南省",
                "四川省",
                "贵州省",
                "云南省",
                "北京市",
                "天津市",
                "上海市",
                "重庆市",
                "内蒙古自治区",
                "新疆维吾尔自治区",
                "夏回族自治区",
                "广西壮族自治区",
                "西藏自治区",
                "香港特别行政区",
                "澳门特别行政区" };

        Stream<String> s = Stream.of(provinces);
        test1(s);
        test2(s);
    }

    public static void test1(Stream<String> s) {
        System.out.println(s.filter(p -> p.length() == 3).count());
    }

    public static void test2(Stream<String> s) {
        System.out.println(s.filter(p -> p.contains("东") || p.contains("南") || p.contains("西") || p.contains("北")).count());
    }
}
