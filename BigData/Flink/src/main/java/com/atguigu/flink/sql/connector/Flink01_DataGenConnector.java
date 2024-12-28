package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink01_DataGenConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 流表环境
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        // 表环境
        TableEnvironment tableEnv = TableEnvironment.create(EnvironmentSettings.newInstance().build());
        // 通过连接器创建表
        streamTableEnv.executeSql(
                "create table t1(id string, vc int, ts bigint) " +
                        "with(" +
                        "'connector'='datagen', " +
                        "'rows-per-second'='2', " +
                        "'number-of-rows'='100', " +
                        "'fields.id.kind'='random', " +
                        "'fields.id.length'='6', " +
                        "'fields.vc.kind'='random', " +
                        "'fields.vc.min'='1000', " +
                        "'fields.vc.max'='2000', " +
                        "'fields.ts.kind'='sequence', " +
                        "'fields.ts.start'='100', " +
                        "'fields.ts.end'='200'" +
                        ")"
        );
        streamTableEnv.sqlQuery("select * from t1").execute().print();
    }
}
