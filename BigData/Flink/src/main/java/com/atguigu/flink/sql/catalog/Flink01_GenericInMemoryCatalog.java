package com.atguigu.flink.sql.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink01_GenericInMemoryCatalog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 获取当前使用的catalog
        String currentCatalog = streamTableEnv.getCurrentCatalog();
        System.out.println(currentCatalog);
        // 获取当前使用的database
        String currentDatabase = streamTableEnv.getCurrentDatabase();
        System.out.println(currentDatabase);

        streamTableEnv.executeSql(
                "create table t_source(" +
                        "id string, " +
                        "vc int, " +
                        "ts bigint " +
                        ") " +
                        "with(" +
                        "'connector'='filesystem', " +
                        "'path'='D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\input\\ws.txt', " +
                        "'format'='csv', " +
                        "'csv.field-delimiter'=',' " +
                        ")"
        );
        streamTableEnv.sqlQuery("select * from default_catalog.default_database.t_source").execute().print();
    }
}
