package com.atguigu.flink.pojo;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/*
Flink对POJO要求
1. 类必须是public
2. 类中必须提供无参构造器
3. 类中的属性必须能被访问
  3.1 属性直接使用public修饰
  3.2 属性使用private修饰，但要提供public修饰的get/set方法
4. 类中属性必须可序列化
 * */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class WordCount {
    private String word;
    private Long count;

    @Override
    public String toString() {
        return word + " : " + count;
    }
}
