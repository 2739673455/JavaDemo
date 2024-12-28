package com.atguigu.springbootdemo.customer.controller;

import com.atguigu.springbootdemo.customer.bean.Customer;
import com.atguigu.springbootdemo.customer.service.CustomerService;
import com.baomidou.dynamic.datasource.annotation.DS;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.*;

import java.time.LocalTime;

/*
控制层的职责:
    接受来自客户端的请求和参数
    调用服务层
    返回响应结果
*/

//控制层的组件
//RestController: 将类标识为控制层组件，Spring的IOC容器会管理该组件的对象(容器会创建该组件的对象并将对象管理到容器中)
@RestController
@DS("db1")
public class CustomerController {

    @Autowired
    @Qualifier("csi")
    CustomerService customerService;

    /*
    请求参数的处理:
        键值对参数
            客户端请求: localhost:8080/param1?username=zhangsan&age=20
            @RequestParam: 将请求中的键值对参数映射到请求处理方法的形参上
        请求路径中的参数
            客户端的请求: http://localhost:8080/param2/zhangsan/20
            @PathVariable: 将请求路径中的参数映射到请求处理方法的形参上
        请求体参数
            客户端的请求: http://localhost:8080/param3
            请求体参数: {"id":1001, "username":"wangwu" , "age":23}
            @RequestBody: 将请求体中的参数封装到对象的属性上，要求请求参数名与对象的属性名一致

    响应结果的格式:
        通常以json格式响应

    常见状态码:
        200 : Ok
        400 : 请求参数有误
        404 : 请求资源不存在
        405 : 请求方式不正确
        500 : 服务器内部错误
    */

    @RequestMapping(value = "/hello")
    public String hello() {
        System.out.println("hello");
        return "hello";
    }

    @RequestMapping(value = "/param1")
    public String param1(@RequestParam(value = "username") String username, @RequestParam(value = "password") String password) {
        System.out.println("param1");
        return "param1";
    }

    @RequestMapping(value = "/param2/{username}/{age}")
    public String param2(@PathVariable(value = "username") String username, @PathVariable(value = "age") Integer age) {
        System.out.println("param2");
        return "param2";
    }

    @RequestMapping(value = "/param3")
    public Customer param3(@RequestBody Customer customer) {
        System.out.println(customer);
        return customer;
    }

    @GetMapping(value = "/getcustomer")
    public Customer getCustomer(@RequestBody Customer customer) {
        customer = customerService.getCustomer(customer.getId());
        System.out.println("select " + customer + " at " + LocalTime.now());
        return customer;
    }

    @PostMapping(value = "/savecustomer")
    public String saveCustomer(@RequestBody Customer customer) {
        customerService.saveCustomer(customer);
        String result = "insert " + customer;
        System.out.println(result + " at " + LocalTime.now());
        return result;
    }

    @PostMapping(value = "/updatecustomer")
    public String updateCustomer(@RequestBody Customer customer) {
        customerService.updateCustomer(customer);
        String result = "update " + customer;
        System.out.println(result + " at " + LocalTime.now());
        return result;
    }

    @RequestMapping(value = "/deletecustomer")
    public String deleteCustomer(@RequestBody Customer customer) {
        customerService.deleteCustomer(customer.getId());
        String result = "delete " + customer;
        System.out.println(result + " at " + LocalTime.now());
        return result;
    }

    @PostMapping(value = "/get")
    public Customer getMP(@RequestBody Customer customer) {
        customer = customerService.getById(customer.getId());
        System.out.println("select " + customer + " at " + LocalTime.now());
        return customer;
    }

    @PostMapping(value = "/insert")
    public String insertMP(@RequestBody Customer customer) {
        customerService.save(customer);
        String result = "insert " + customer;
        System.out.println(result + " at " + LocalTime.now());
        return result;
    }

    @PostMapping(value = "/update")
    public String updateMP(@RequestBody Customer customer) {
        customerService.updateById(customer);
        String result = "update " + customer;
        System.out.println(result + " at " + LocalTime.now());
        return result;
    }

    @PostMapping(value = "/delete")
    public String deleteMP(@RequestBody Customer customer) {
        customerService.removeById(customer.getId());
        String result = "delete " + customer;
        System.out.println(result + " at " + LocalTime.now());
        return result;
    }
}