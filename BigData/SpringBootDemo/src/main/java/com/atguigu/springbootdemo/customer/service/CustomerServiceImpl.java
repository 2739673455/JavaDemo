package com.atguigu.springbootdemo.customer.service;

import com.atguigu.springbootdemo.customer.bean.Customer;
import com.atguigu.springbootdemo.customer.mapper.CustomerMapper;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/*
业务层的组件
Service:将类标识为业务层组件，Spring的IOC容器会管理该组件的对象(容器会创建该组件的对象并将对象管理到容器中)
默认情况下组件被管理到容器中后会有一个默认名字: 类名首字母小写，也可以在注解中明确指定名字(@Service(value="name"))
*/

@Service(value = "csi")
@DS("db1")
public class CustomerServiceImpl extends ServiceImpl<CustomerMapper, Customer> implements CustomerService {

    @Autowired
    CustomerMapper customerMapper;

    @Override
    public Customer getCustomer(Integer id) {
        //调用自己的方法
        Customer customer = customerMapper.getCustomer(id);
        //调用MP的方法
        //Customer customer = customerMapper.selectById(id);
        return customer;
    }

    @Override
    public void saveCustomer(Customer customer) {
        //调用自己的方法
        customerMapper.saveCustomer(customer);
        //调用MP的方法
        //customerMapper.insert(customer);
    }

    @Override
    public void updateCustomer(Customer customer) {
        //调用自己的方法
        customerMapper.updateCustomer(customer);
        //调用MP的方法
        //customerMapper.updateById(customer);
    }

    @Override
    public void deleteCustomer(Integer id) {
        //调用自己的方法
        customerMapper.deleteCustomer(id);
        //调用MP的方法
        //customerMapper.deleteById(id);
        //baseMapper.deleteById(id);
    }
}
