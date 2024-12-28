package com.atguigu.homework6;

import java.util.Arrays;

public class MyArrayList<T> {
    private Object[] all = new Object[4];
    private int total;

    public void add(T t) {
        if (total == all.length) {
            Object[] newAll = new Object[all.length * 2];
            System.arraycopy(all, 0, newAll, 0, all.length);
            all = newAll;
        }
        all[total++] = t;
    }

    public T get(int index) {
        return (T) all[index];
    }

    public Object[] toArray() {
        return Arrays.copyOf(all, total);
    }

}
