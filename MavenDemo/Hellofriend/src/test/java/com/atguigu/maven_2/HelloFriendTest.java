package com.atguigu.maven_2;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class HelloFriendTest {

    @Test
    public void test(){
        String results = new HelloFriend().sayHelloToFriend("maven");

        assertEquals("Hello maven!My Name",results);
    }
}
