package com.atguigu.constructor;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class NewConstructor {
    public static void main(String[] args) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchFieldException {
        Class<?> c = Class.forName("com.atguigu.constructor.Person");
        Constructor<?> constructor1 = c.getDeclaredConstructor();
        Field whoField = c.getDeclaredField("hp");
        Field nameField = c.getDeclaredField("name");
        Field ageField = c.getDeclaredField("age");
        constructor1.setAccessible(true);
        whoField.setAccessible(true);
        nameField.setAccessible(true);
        ageField.setAccessible(true);

        whoField.set(null, "hp");

        Object o1 = constructor1.newInstance();//通过构造器实例化
        nameField.set(o1, "张三");
        ageField.set(o1, 89);

        Object o2 = c.newInstance();//通过类实例化
        nameField.set(o2, "李四");
        ageField.set(o2, 19);

        Constructor<?> constructor2 = c.getDeclaredConstructor(String.class, int.class);
        constructor2.setAccessible(true);
        Object o3 = constructor2.newInstance("王五", 423);

        System.out.println(o1);
        System.out.println(o2);
        System.out.println(o3);

        Method method1 = c.getMethod("max", int.class, int.class);
        Object result1 = method1.invoke(null, 42, 89);
        System.out.println(result1);

        Method method2 = c.getMethod("getNameAge");
        Object result2 = method2.invoke(o2);
        System.out.println(result2);
    }
}
