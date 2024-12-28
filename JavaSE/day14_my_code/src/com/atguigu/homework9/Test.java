package com.atguigu.homework9;

import java.util.Scanner;

public class Test {
    public static void main(String[] args) {
        UserManager um = new UserManager();
        Scanner input = new Scanner(System.in);

        while (true) {
            showMenu();
            int choice = input.nextInt();
            if (choice == 1) {
                System.out.print("请输入注册用户名与密码");
                User usr = new User(input.next(), input.next());
                try {
                    um.checkUsernameExists(usr.getName());
                    um.add(usr);
                    um.showUserList();
                    System.out.println("注册成功");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else if (choice == 2) {
                System.out.print("请输入登录用户名与密码");
                User usr = new User(input.next(), input.next());
                try {
                    um.login(usr);
                    System.out.println("登陆成功");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                break;
            }
        }
        input.close();
    }


    public static void showMenu() {
        System.out.println("--------尚硅谷-------");
        System.out.println("|      1、注册      |");
        System.out.println("|      2、登录      |");
        System.out.println("|      3、退出      |");
        System.out.println("--------------------");
    }
}
