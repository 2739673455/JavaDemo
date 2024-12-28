package com.atguigu.getclass;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;

public class TestClass {
    public static void main(String[] args) throws NoSuchFieldException, NoSuchMethodException {
        Class c1 = Student.class;
        System.out.println(c1.getName());
        System.out.println(c1.getPackageName());
        System.out.println(c1.getModifiers());
        System.out.println(Modifier.toString(c1.getModifiers()));
        System.out.println(c1.getSuperclass());
        System.out.println(Arrays.toString(c1.getInterfaces()));

        System.out.println("成员属性");
        Field[] fields = c1.getDeclaredFields();
        for (Field field : fields) {
            System.out.println(field);
        }

        System.out.println("构造器");
        Constructor[] constructors = c1.getDeclaredConstructors();
        for (Constructor constructor : constructors) {
            System.out.println(constructor);
        }

        System.out.println("成员方法");
        Method[] methods = c1.getDeclaredMethods();
        for (Method method : methods) {
            System.out.println(method);
        }

        System.out.println("内部类");
        Class[] classes = c1.getDeclaredClasses();
        for (Class class1 : classes) {
            System.out.println(class1);
        }

//        System.out.println("构造器1");
//        Constructor constructor1 = c1.getDeclaredConstructor();
//        System.out.println(constructor1);

        System.out.println("构造器2");
        Constructor constructor2 = c1.getConstructor(String.class);
        System.out.println(constructor2);
    }
}
