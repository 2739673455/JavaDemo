package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink04_JDBCConnector {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 读取
        streamTableEnv.executeSql(
                "create table t_source(id string, vc bigint, ts bigint) " +
                        "with(" +
                        "'connector'='jdbc'," +
                        "'driver'='com.mysql.cj.jdbc.Driver'," +
                        "'url'='jdbc:mysql://localhost:3306/db1'," +
                        "'table-name'='watersensor'," +
                        "'username'='root'," +
                        "'password'='123321'" +
                        ")"
        );
        // 写入
        //streamTableEnv.executeSql(
        //        "create table t_sink(id string, vc bigint, ts bigint) " +
        //                "with(" +
        //                "'connector'='jdbc'," +
        //                "'driver'='com.mysql.cj.jdbc.Driver'," +
        //                "'url'='jdbc:mysql://localhost:3306/db1'," +
        //                "'table-name'='watersensor2'," +
        //                "'username'='root'," +
        //                "'password'='123321'" +
        //                ")"
        //);
        //Table table = streamTableEnv.sqlQuery("select * from t_source");
        //table.executeInsert("t_sink");
        // 写入时指定主键
        streamTableEnv.executeSql(
                "create table t_sink(id string, vc bigint, ts bigint, primary key (id) not enforced) " +
                        "with(" +
                        "'connector'='jdbc'," +
                        "'driver'='com.mysql.cj.jdbc.Driver'," +
                        "'url'='jdbc:mysql://localhost:3306/db1'," +
                        "'table-name'='watersensor3'," +
                        "'username'='root'," +
                        "'password'='123321'" +
                        ")"
        );
        Table tableWithKey = streamTableEnv.sqlQuery("select * from t_source");
        tableWithKey.executeInsert("t_sink");
    }
}
