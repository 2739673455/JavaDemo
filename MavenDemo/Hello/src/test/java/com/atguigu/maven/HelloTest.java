package com.atguigu.maven;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HelloTest {
    
    @Test
    public void sayHelloTest(){
        String str = new Hello().sayHello("atguigu");

        //断言
        assertEquals("Hello atguigu!",str);
    }
}
