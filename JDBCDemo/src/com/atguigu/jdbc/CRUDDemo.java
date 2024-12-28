package com.atguigu.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CRUDDemo {
    public static void main(String[] args) throws SQLException {
        //insert();
        //update();
        //del();
        List<Emp> emps = search();
        for (Emp emp : emps) {
            System.out.println(emp);
        }
    }

    public static void insert() throws SQLException {
        Connection connection = JDBCUtils.getConnection();
        String sql = "insert into emp(id,name,salary) values(?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setInt(1, 1);
        ps.setString(2, "zhangsan");
        ps.setDouble(3, 9.9);
        int result = ps.executeUpdate(); // 该方法用来执行增、删、改的SQL语句
        System.out.println("共" + result + "行受到影响");
        JDBCUtils.close(ps, connection);
    }

    public static void update() throws SQLException {
        Connection connection = JDBCUtils.getConnection();
        String sql = "update emp set name=?,salary=? where id=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, "lisi");
        ps.setDouble(2, 999.99);
        ps.setInt(3, 1);
        int result = ps.executeUpdate();
        System.out.println("共" + result + "行受到影响");
        JDBCUtils.close(ps, connection);
    }

    public static void del() throws SQLException {
        Connection connection = JDBCUtils.getConnection();
        String sql = "delete from emp where id=?";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setInt(1, 1);
        int result = ps.executeUpdate();
        System.out.println("共" + result + "行受到影响");
        JDBCUtils.close(ps, connection);
    }


    public static List<Emp> search() throws SQLException {
        List<Emp> empList = new ArrayList<Emp>();
        Connection connection = JDBCUtils.getConnection();
        String sql = "select * from emp";
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            int id = rs.getInt("id");
            String name = rs.getString("name");
            double salary = rs.getDouble("salary");
            empList.add(new Emp(id, name, salary));
        }
        JDBCUtils.close(ps, connection, rs);
        return empList;
    }
}
