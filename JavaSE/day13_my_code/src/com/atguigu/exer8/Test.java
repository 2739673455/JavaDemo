package com.atguigu.exer8;

public class Test {
    public static void main(String[] args) {
        Rest[] arr = new Rest[3];
        arr[0] = new Rest() {
            @Override
            public void rest() {
                System.out.println("休息就是睡大觉");
            }
        };
        arr[1] = new Rest() {
            @Override
            public void rest() {
                System.out.println("休息就是到处浪");
            }
        };
        arr[2] = new Rest() {
            @Override
            public void rest() {
                System.out.println("休息就是偷偷学");
            }
        };
        for (Rest rest : arr) {
            rest.rest();
        }
    }
}
