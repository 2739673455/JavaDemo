package com.atguigu.homework1;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

public class TestReflect {
    public static void main(String[] args) throws Exception {
        test01();
        test02();
        test03();
        test04();

    }

    public static void test01() throws Exception {
        Class atGuiGuStudentClass = Class.forName("com.atguigu.homework1.AtGuiGuStudent");
        ClassLoader classLoader = atGuiGuStudentClass.getClassLoader();
        Package pkg = atGuiGuStudentClass.getPackage();
        Class cls = atGuiGuStudentClass.getClass();
        Class spcls = atGuiGuStudentClass.getSuperclass();
        Class[] itfs = atGuiGuStudentClass.getInterfaces();
        Field[] fds = atGuiGuStudentClass.getDeclaredFields();
        Constructor[] csts = atGuiGuStudentClass.getDeclaredConstructors();
        Method[] mtds = atGuiGuStudentClass.getDeclaredMethods();

        System.out.println("类加载器：" + classLoader);
        System.out.println("包：" + pkg);
        System.out.println("类：" + cls);
        System.out.println("父类：" + spcls);
        System.out.println("父接口：");
        for (Class c : itfs) {
            System.out.println(c);
        }
        System.out.println("属性：");
        for (Field c : fds) {
            System.out.println(c);
        }
        System.out.println("构造器：");
        for (Constructor c : csts) {
            System.out.println(c);
        }
        System.out.println("方法：");
        for (Method c : mtds) {
            System.out.println(c);
        }
    }

    public static void test02() throws Exception {
        Class atGuiGuStudentClass = Class.forName("com.atguigu.homework1.AtGuiGuStudent");
        Field schoolField = atGuiGuStudentClass.getDeclaredField("school");
        schoolField.setAccessible(true);
        schoolField.set(null, "尚硅谷");
        System.out.println(schoolField.get(null));
    }

    public static void test03() throws Exception {
        Class atGuiGuStudentClass = Class.forName("com.atguigu.homework1.AtGuiGuStudent");
        Constructor constructor = atGuiGuStudentClass.getDeclaredConstructor();
        Field classNameField = atGuiGuStudentClass.getDeclaredField("className");
        constructor.setAccessible(true);
        classNameField.setAccessible(true);
        Object o1 = constructor.newInstance();
        Object o2 = constructor.newInstance();
        classNameField.set(o1, "0522");
        classNameField.set(o2, "0499");
        System.out.println(classNameField.get(o1));
        System.out.println(classNameField.get(o2));
    }

    public static void test04() throws Exception {
        Class atGuiGuStudentClass = Class.forName("com.atguigu.homework1.AtGuiGuStudent");
        Constructor constructor = atGuiGuStudentClass.getDeclaredConstructor(String.class);
        constructor.setAccessible(true);
        Object o1 = constructor.newInstance("0522");
        Object o2 = constructor.newInstance("0499");
        Method method = atGuiGuStudentClass.getDeclaredMethod("compareTo", AtGuiGuStudent.class);
        Object result = method.invoke(o1, o2);
        System.out.println(result);
    }
}
