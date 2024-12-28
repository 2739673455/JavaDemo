package com.atguigu.flink.sql.catalog;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/*
可使用hive中已有的表，也支持将表创建在hive中
* */
public class Flink03_HiveCatalog {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        // 使用HiveCatalog
        HiveCatalog hiveCatalog = new HiveCatalog(
                "hive",
                "default",
                "D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\conf"
        );
        streamTableEnv.registerCatalog("hive", hiveCatalog);
        streamTableEnv.useCatalog("hive");

        streamTableEnv.executeSql(
                "create table t2(" +
                        "id string, " +
                        "vc int, " +
                        "ts bigint " +
                        ")with (" +
                        " 'connector' = 'filesystem', " +
                        " 'path' = 'D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\input\\ws.txt', " +
                        " 'format' = 'csv'" +
                        ")"
        );
        streamTableEnv.sqlQuery("select * from t2").execute().print();
    }
}
