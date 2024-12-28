package com.atguigu.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;

public class TestDemo {
    public static void main(String[] args) throws Exception {
        Class c = Demo.class;
        Method method = c.getDeclaredMethod("fun");
        Annotation[] annotations = method.getDeclaredAnnotations();
        for (Annotation annotation : annotations) {
            if (annotation instanceof OneAnnotation one) {
                System.out.println(one.m1());
                System.out.println(one.m2());
            }
        }
    }
}
