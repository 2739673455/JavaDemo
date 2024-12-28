package com.atguigu.annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface OneAnnotation {
    String m1() default "method1";

    String m2();
}
