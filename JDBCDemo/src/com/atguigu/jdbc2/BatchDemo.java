package com.atguigu.jdbc2;

import com.atguigu.jdbc.JDBCUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class BatchDemo {
    public static void main(String[] args) throws SQLException {
        Connection connection = JDBCUtils.getConnection();
        String sql = "insert into emp(id,name,salary) values(?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        for (int i = 1; i <= 100000; i++) {
            ps.setInt(1, i);
            ps.setString(2, "" + i);
            ps.setDouble(3, i);
            ps.addBatch();
            if (i % 1000 == 0) {
                ps.executeBatch();
                ps.clearBatch();
            }
        }
        JDBCUtils.close(ps, connection);
    }
}
