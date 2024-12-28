package com.atguigu.springbootdemo.customer.mapper;

import com.atguigu.springbootdemo.customer.bean.Customer;
import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.*;


/*
@Mapper: 将该接口的实现类标识为数据层组件
         MyBatis在运行时会通过动态代理的方式给接口生成代理实现类

SQL中的占位符:
    #{} : 相对智能，会自动给字符串类型的字段添加''，可以提取参数对象中的属性等
    ${} : 原样显示，一般用于将提前处理好的sql语句原样显示
*/

@Mapper
@DS("db1")
public interface CustomerMapper extends BaseMapper<Customer> {
    @Select("select id, username, age from customer where id = #{id}")
    Customer getCustomer(Integer id);

    @Insert("insert into customer(username,age) values (#{customer.username},#{customer.age})")
    void saveCustomer(@Param("customer") Customer customer);

    @Update("update customer set username = #{username}, age = #{age} where id = #{id}")
    void updateCustomer(Customer customer);

    @Delete("delete from customer where id = #{id}")
    void deleteCustomer(Integer id);
}
