package com.atguigu.edu.common.util;

import com.atguigu.edu.common.constant.Constant;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JdbcUtil {
    public static Connection getMySqlConnection() throws Exception {
        Class.forName(Constant.MYSQL_DRIVER);
        return DriverManager.getConnection(Constant.MYSQL_URL, Constant.MYSQL_USER_NAME, Constant.MYSQL_PASSWORD);
    }

    public static void closeMySqlConnection(Connection connection) throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    // 通用的从MySql表中查询数据
    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clazz) throws Exception {
        return queryList(connection, sql, clazz, false);
    }

    public static <T> List<T> queryList(Connection connection, String sql, Class<T> clazz, boolean isUnderLineToCamel) throws Exception {
        List<T> result = new ArrayList<>();
        // 获取数据库操作对象
        PreparedStatement ps = connection.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            // 定义一个对象，用于封装查询结果
            T t = clazz.newInstance();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                Object columnValue = rs.getObject(columnName);
                // 是否转换为驼峰命名
                if (isUnderLineToCamel)
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, columnName);
                // 给对象属性赋值
                BeanUtils.setProperty(t, columnName, columnValue);
            }
            result.add(t);
        }
        rs.close();
        ps.close();
        return result;
    }
}
