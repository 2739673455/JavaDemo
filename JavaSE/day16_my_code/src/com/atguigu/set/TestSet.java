package com.atguigu.set;

import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.TreeSet;

public class TestSet {

    @Test
    public void test() {
        HashSet hset = new HashSet();
        hset.add("c");
        hset.add("a");
        hset.add("b");
        hset.add("a");
        System.out.println(hset);

        LinkedHashSet lset = new LinkedHashSet();
        lset.add("a");
        lset.add("b");
        lset.add("c");
        lset.add("a");
        System.out.println(lset);

        TreeSet tset = new TreeSet();
        tset.add("a");
        tset.add("b");
        tset.add("c");
        tset.add("a");
        System.out.println(tset);
    }
}
