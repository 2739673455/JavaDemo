package com.atguigu.flink.sql.module;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.module.hive.HiveModule;

public class Flink01_module {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        HiveModule hiveModule = new HiveModule("3.1.3");
        streamTableEnv.loadModule("hive", hiveModule);

        // Flink核心Module: core
        // Hive Module: hive
        // 使用顺序
        streamTableEnv.useModules("core", "hive");

        streamTableEnv.sqlQuery("select split('a,bb,ccc,dddd',',')").execute().print();
    }
}
