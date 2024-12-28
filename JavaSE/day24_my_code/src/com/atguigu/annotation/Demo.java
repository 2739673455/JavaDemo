package com.atguigu.annotation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@OneAnnotation(m2 = "method2")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Demo {
    private int num;

    @OneAnnotation(m2 = "fun-method2")
    public Object fun() {
        System.out.println("num" + num);
        return null;
    }
}
