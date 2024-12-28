package com.atguigu.jdbc2;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.Properties;


public class DruidDemo {
    public static void main(String[] args) throws Exception {
        test1();
    }

    public static void test1() throws Exception {
        Properties properties = new Properties();
        properties.load(Files.newInputStream(Paths.get("druid.properties")));
        //2.创建数据库连接池对象
        DataSource dataSource = DruidDataSourceFactory.createDataSource(properties);
        //3.获取Connection对象
        Connection connection = dataSource.getConnection();
        System.out.println(connection);
        //4.关闭资源
        connection.close();
    }
}
