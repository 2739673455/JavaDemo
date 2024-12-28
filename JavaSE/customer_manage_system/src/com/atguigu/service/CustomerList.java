package com.atguigu.service;

import com.atguigu.bean.Customer;
import com.atguigu.view.CMUtility;

public class CustomerList {
    private static Customer[] all = new Customer[3];
    private static int count = 0;

    public static void addCustomer(Customer c) {
        if (count == all.length) {
            Customer[] newCustomer = new Customer[all.length + 5];
            for (int i = 0; i < all.length; i++) {
                newCustomer[i] = all[i];
            }
            all = newCustomer;
        }
        all[count] = c;
        count++;
    }

    public static boolean modifyCustomer(int id) {
        if (id >= count || id < 0) {
            return false;
        } else {
            Customer customer = all[id];
            System.out.print("姓名" + "(" + customer.getName() + "):");
            customer.setName(CMUtility.readString(5, customer.getName()));
            System.out.print("性别:" + "(" + customer.getGender() + "):");
            customer.setGender(CMUtility.readChar(customer.getGender()));
            System.out.print("年龄:" + "(" + customer.getAge() + "):");
            customer.setAge(CMUtility.readInt(customer.getAge()));
            System.out.print("电话:" + "(" + customer.getPhone() + "):");
            customer.setPhone(CMUtility.readString(11, customer.getPhone()));
            System.out.print("邮箱:" + "(" + customer.getEmail() + "):");
            customer.setEmail(CMUtility.readString(32, customer.getEmail()));
            return true;
        }
    }

    public static boolean deleteCustomer(int id) {
        if (id >= count || id < 0) {
            return false;
        } else {
            for (int i = id; i < count - 1; i++) {
                all[i] = all[i + 1];
            }
            all[count - 1] = null;
            count--;
            return true;
        }
    }

    public static Customer[] getAllCustomers() {
        Customer[] newCustomers = new Customer[count];
        for (int i = 0; i < count; i++) {
            newCustomers[i] = all[i];
        }
        return newCustomers;
    }

    public static Customer getCustomer(int id) {
        return all[id];
    }
}
