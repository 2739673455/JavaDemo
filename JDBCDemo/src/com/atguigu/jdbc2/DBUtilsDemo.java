package com.atguigu.jdbc2;

import com.atguigu.jdbc.Emp;
import com.atguigu.jdbc.JDBCUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.BeanListHandler;

import java.sql.SQLException;
import java.util.List;

public class DBUtilsDemo {
    public static void main(String[] args) throws SQLException {
        //test1();
        test2();
    }

    public static void test1() throws SQLException {
        QueryRunner qr = new QueryRunner();
        int result = qr.update(JDBCUtils.getConnection(), "insert into emp values(?,?,?)", 4, "lisi", 0.5);
        System.out.println("共" + result + "行受到影响");
    }


    public static void test2() throws SQLException {
        QueryRunner qr = new QueryRunner();
        //Emp query = qr.query(JDBCUtils.getConnection(), "select * from emp", new BeanHandler<Emp>(Emp.class));
        //System.out.println(query);
        List<Emp> query = qr.query(JDBCUtils.getConnection(), "select * from emp", new BeanListHandler<Emp>(Emp.class));
        for (Emp emp : query)
            System.out.println(emp);
    }
}

