package com.atguigu.streamclass;

import java.util.stream.Stream;

public class TestStream {
    public static void main(String[] args) {
        test01();
    }

    public static void test01() {
        Stream s1 = Stream.of("🐕", "🐱", "🐀", "🐒", "🐅");
        s1.map(s -> s += "🍌").forEach(System.out::println);
    }
}
