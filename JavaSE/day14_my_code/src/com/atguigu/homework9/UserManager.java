package com.atguigu.homework9;

import javax.security.auth.login.LoginException;
import java.util.Objects;

public class UserManager {
    private static User[] arr;
    private static int pos;
    private static int total;

    UserManager() {
        total = 2;
        arr = new User[total];
    }

    public void checkUsernameExists(String username) throws Exception {
        for (int i = 0; i < pos; i++) {
            if (Objects.equals(username, arr[i].getName())) {
                throw new UsernameAlreadyExistsException("用户已存在");
            }
        }
    }

    public void add(User user) throws Exception {
        if (pos == total) {
            total += total / 2;
            arrExtend();
        }
        arr[pos++] = user;
    }

    public void login(User user) throws Exception {
        for (int i = 0; i < pos; i++) {
            if (Objects.equals(arr[i].getName(), user.getName()) && Objects.equals(arr[i].getPassword(), user.getPassword())) {
                return;
            }
        }
        throw new LoginException("用户名或密码错误");
    }

    public void arrExtend() {
        User[] arrNew = new User[total];
        for (int i = 0; i < pos; i++) {
            arrNew[i] = arr[i];
        }
        arr = arrNew;
    }

    public void showUserList() {
        for (int i = 0; i < pos; i++) {
            System.out.println(arr[i]);
        }
    }
}
