package com.atguigu.springbootdemo.customer.bean;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@TableName(value = "customer")
public class Customer {
    @TableId(value = "id", type = IdType.AUTO)
    private int id;
    private String username;
    private int age;
}
