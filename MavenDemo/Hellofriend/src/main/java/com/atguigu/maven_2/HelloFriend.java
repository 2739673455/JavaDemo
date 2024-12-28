package com.atguigu.maven_2;

import com.atguigu.maven.Hello;

public class HelloFriend {
    public String sayHelloToFriend(String name) {
        Hello hello = new Hello();
        return hello.sayHello(name)+ this.getMyName();
    }

    public String getMyName(){
        return "My Name";
    }
}
