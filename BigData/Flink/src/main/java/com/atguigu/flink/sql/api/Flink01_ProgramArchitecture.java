package com.atguigu.flink.sql.api;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
程序架构
    1. 准备执行环境
        1. 流表环境
        2. 表环境
    2. 创建表
        1. 流转表，只能使用流表环境进行转换
        2. 连接器表，通过创建连接器表，直接对接外部组件
    3. 对表中数据处理
        1. TableAPI
        2. SQL
    4. 输出结果
        1. 表转流，基于流的方式输出
        2. 连接器表，通过创建连接器表，直接对接外部组件
 * */
public class Flink01_ProgramArchitecture {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 1.1 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 1.2 表环境
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inStreamingMode().build());
        //TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());

        env.execute();
    }
}
