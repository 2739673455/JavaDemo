package com.atguigu.view;

import com.atguigu.bean.Customer;
import com.atguigu.service.CustomerList;

public class CustomerView {
    public static void main(String[] args) {
        while (true) {
            showMenu();
            if (selectOption()) {
                break;
            }
        }
    }

    public static void showMenu() {
        System.out.println("-----------------拼电商客户管理系统-----------------");
        System.out.println("\t\t1 添 加 客 户");
        System.out.println("\t\t2 修 改 客 户");
        System.out.println("\t\t3 删 除 客 户");
        System.out.println("\t\t4 客 户 列 表");
        System.out.println("\t\t5 退    出");
        System.out.print("请选择(1-5)：");
    }

    public static boolean selectOption() {
        char select = CMUtility.readMenuSelection();
        switch (select) {
            case '1':
                addCustomer();
                break;
            case '2':
                showCustomerList();
                modifyCustomer();
                break;
            case '3':
                showCustomerList();
                deleteCustomer();
                break;
            case '4':
                showCustomerList();
                break;
            case '5':
                return true;
        }
        return false;
    }

    public static Customer setNewCustomer() {
        Customer customer = new Customer();
        System.out.print("姓名:");
        customer.setName(CMUtility.readString(5));
        System.out.print("性别:");
        customer.setGender(CMUtility.readChar());
        System.out.print("年龄:");
        customer.setAge(CMUtility.readInt());
        System.out.print("电话:");
        customer.setPhone(CMUtility.readString(11));
        System.out.print("邮箱:");
        customer.setEmail(CMUtility.readString(32));
        return customer;
    }

    public static void addCustomer() {
        System.out.println("1.添加客户:");
        CustomerList.addCustomer(setNewCustomer());
        System.out.println("添加完毕");
    }

    public static void modifyCustomer() {
        System.out.print("请输入要修改的序号：");
        int id = CMUtility.readInt();
        System.out.println("2.修改客户:");
        if (CustomerList.modifyCustomer(id - 1)) {
            System.out.println("修改完毕");
        } else {
            System.out.println("输入序号有误");
        }
    }

    public static void deleteCustomer() {
        System.out.print("请输入要删除的序号：");
        int id = CMUtility.readInt();
        System.out.println("3.删除客户:");
        if (CustomerList.deleteCustomer(id - 1)) {
            System.out.println("删除完毕");
        } else {
            System.out.println("输入序号有误");
        }
    }

    public static void showCustomerList() {
        System.out.println("序号\t姓名\t性别\t年龄\t电话\t\t邮箱");
        Customer[] all = CustomerList.getAllCustomers();
        for (int i = 0; i < all.length; i++) {
            System.out.println(i + 1 + "\t" + all[i]);
        }
    }

}
