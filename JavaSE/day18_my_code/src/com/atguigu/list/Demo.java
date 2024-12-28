package com.atguigu.list;

import org.junit.Test;

import java.util.LinkedList;

public class Demo {
    @Test
    public void test() {
        LinkedList<String> list = new LinkedList<>();
        list.add("a");
        list.add("b");
        list.addFirst("f");
        list.addLast("g");
        list.push("h");


        System.out.println(list.remove());
        System.out.println(list.poll());
        System.out.println(list.pollLast());
        System.out.println(list.remove());
        System.out.println(list.pop());

    }
}
