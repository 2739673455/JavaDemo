package com.atguigu.flink.sql.catalog;

import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Arrays;

public class Flink02_JDBCCatalog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 使用JDBCCatalog
        JdbcCatalog jdbcCatalog = new JdbcCatalog(
                Flink02_JDBCCatalog.class.getClassLoader(),
                "jdbc",
                "db1",
                "root",
                "123321",
                "jdbc:mysql://localhost:3306");
        streamTableEnv.registerCatalog("jdbc", jdbcCatalog);
        streamTableEnv.useCatalog("jdbc");

        // 获取所有表
        String[] tables = streamTableEnv.listTables();
        System.out.println(Arrays.toString(tables));

        streamTableEnv.sqlQuery("select * from url_view_count").execute().print();
    }
}
