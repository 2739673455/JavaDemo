package com.atguigu.jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class JDBCUtils {
    static String user;
    static String password;
    static String url;
    static String driverClassName;

    static {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream("jdbc.properties");
            Properties properties = new Properties();
            properties.load(fis);
            user = properties.getProperty("user");
            password = properties.getProperty("password");
            url = properties.getProperty("url");
            driverClassName = properties.getProperty("driverClassName");
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public static Connection getConnection() {
        try {
            Class.forName(driverClassName);
            return DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public static void close(PreparedStatement ps, Connection connection) {
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void close(PreparedStatement ps, Connection connection, ResultSet rs) {
        close(ps, connection);
        if (rs != null) {
            try {
                rs.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}

