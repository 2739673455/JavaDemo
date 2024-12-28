package com.atguigu.jdbc2;


import com.atguigu.jdbc.JDBCUtils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class TransactionDemo {
    public static void main(String[] args) throws SQLException {
        Connection connection = JDBCUtils.getConnection();
        String sql = "update account set balance=? where name=?";

        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(sql);
            connection.setAutoCommit(false);

            ps.setInt(1, 6000);
            ps.setString(2, "aa");
            ps.executeUpdate();

            ps.setInt(1, 500);
            ps.setString(2, "cc");
            ps.executeUpdate();
            connection.commit();

            System.out.println("转账成功");
        } catch (SQLException e) {
            connection.rollback();
            System.out.println("转账失败");
        } finally {
            connection.setAutoCommit(true);
            JDBCUtils.close(ps, connection);
        }
    }
}
