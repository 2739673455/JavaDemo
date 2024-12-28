package com.atguigu.homework2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;

public class Demo {
    public static void main(String[] args) throws Exception {
        ArrayList<String> fruits = getFruits();

        Object o1 = instanceClass(fruits.get(0));
        Object o2 = instanceClass(fruits.get(1));
        Object o3 = instanceClass(fruits.get(2));

        Class jucierClass = Class.forName("com.atguigu.homework2.Juicer");
        Constructor jucierCtor = jucierClass.getDeclaredConstructor(Fruit.class);
        Object jucier1 = jucierCtor.newInstance(o1);
        Object jucier2 = jucierCtor.newInstance(o2);
        Object jucier3 = jucierCtor.newInstance(o3);

        new Thread((Juicer) jucier1).start();
        new Thread((Juicer) jucier2).start();
        new Thread((Juicer) jucier3).start();
    }

    public static ArrayList<String> getFruits() throws IOException {
        FileReader fr = new FileReader("day23_my_code/src/com/atguigu/homework2/fruit.properties");
        BufferedReader br = new BufferedReader(fr);
        ArrayList<String> fruits = new ArrayList<>();
        while (br.ready()) {
            String str = br.readLine();
            String[] words = str.split("=");
            fruits.add(words[1]);
        }
        return fruits;
    }

    public static Object instanceClass(String className) throws Exception {
        Class c = Class.forName(className);
        Constructor ctor = c.getDeclaredConstructor();
        Object o = ctor.newInstance();
        return o;
    }
}
