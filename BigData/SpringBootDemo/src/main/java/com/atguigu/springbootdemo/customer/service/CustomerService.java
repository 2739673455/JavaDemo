package com.atguigu.springbootdemo.customer.service;

import com.atguigu.springbootdemo.customer.bean.Customer;
import com.baomidou.mybatisplus.extension.service.IService;

public interface CustomerService extends IService<Customer> {
    Customer getCustomer(Integer id);

    void saveCustomer(Customer customer);

    void updateCustomer(Customer customer);

    void deleteCustomer(Integer id);
}
