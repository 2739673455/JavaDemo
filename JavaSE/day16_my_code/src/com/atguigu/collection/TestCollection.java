package com.atguigu.collection;


import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

public class TestCollection {

    @Test
    public void test() {
        Collection collection = new ArrayList();
        Collection collection2 = new ArrayList();
        collection.add(1);
        collection.add("a");

        collection2.add(2);
        collection2.add("bcd");
        collection2.add("abc");

        collection.addAll(collection2);
        System.out.println(collection);

        Predicate p = new Predicate() {
            @Override
            public boolean test(Object o) {
                if (o instanceof String) {
                    String s = (String) o;
                    return s.contains("a");
                }
                return false;
            }
        };
        collection.remove("a");
        System.out.println(collection);
        collection.removeIf(p);
        System.out.println(collection);
        collection.removeAll(collection2);
        System.out.println(collection);
        collection.retainAll(collection2);
        System.out.println(collection);

        System.out.println(collection.contains(1));
        System.out.println(collection.containsAll(collection2));
        System.out.println(collection.isEmpty());

        Iterator iterator = collection2.iterator();
        while (iterator.hasNext()) {
            System.out.print(iterator.next() + " ");
        }
    }
}
