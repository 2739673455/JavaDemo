package com.atguigu.jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCDemo {
    public static void main(String[] args) throws Exception {
        test1();
//        test2();
//        test3();
    }

    public static void test1() throws SQLException {
        Driver driver = new com.mysql.cj.jdbc.Driver();
        String url = "jdbc:mysql://localhost:3306/myemployees?serverTimezone=Asia/Shanghai";
        Properties info = new Properties();
        info.setProperty("user", "root");
        info.setProperty("password", "123321");
        Connection connection = driver.connect(url, info);
        System.out.println(connection);
    }

    public static void test2() throws SQLException, ClassNotFoundException {
        //方式一
        Driver driver = new com.mysql.cj.jdbc.Driver();
        DriverManager.registerDriver(driver);
        //方式二
        //Class.forName("com.mysql.cj.jdbc.Driver");

        Connection connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/myemployees?serverTimezone=UTC", "root", "123321");
        System.out.println(connection);
    }

    public static void test3() throws SQLException, ClassNotFoundException, IOException {
        FileInputStream fis = new FileInputStream("jdbc.properties");
        Properties properties = new Properties();
        properties.load(fis);
        String user = properties.getProperty("user");
        String password = properties.getProperty("password");
        String url = properties.getProperty("url");
        String driverClassName = properties.getProperty("driverClassName");
        fis.close();

        Class.forName(driverClassName);
        Connection connection = DriverManager.getConnection(url, user, password);
        System.out.println(connection);
    }
}
