package com.atguigu.gmall.realtime.test;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test01_Flink_Doris_SQL {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        // 从Doris读数据
        streamTableEnv.executeSql(
                "CREATE TABLE flink_doris_source ( " +
                        "siteid int, " +
                        "citycode smallint, " +
                        "username string, " +
                        "pv bigint " +
                        ") " +
                        "WITH ( " +
                        "'connector' = 'doris', " +
                        "'fenodes' = 'hadoop102:7030', " +
                        "'table.identifier' = 'test_db.table1', " +
                        "'username' = 'root', " +
                        "'password' = 'aaaaaa' " +
                        ");"
        );
        //streamTableEnv.sqlQuery("select * from flink_doris_source").execute().print();

        // 向Doris写数据
        streamTableEnv.executeSql(
                "CREATE TABLE flink_doris_sink ( " +
                        "siteid int, " +
                        "citycode int, " +
                        "username string, " +
                        "pv bigint " +
                        ") " +
                        "WITH ( " +
                        "'connector' = 'doris', " +
                        "'fenodes' = 'hadoop102:7030', " +
                        "'table.identifier' = 'test_db.table1', " +
                        "'username' = 'root', " +
                        "'password' = 'aaaaaa', " +
                        "'sink.label-prefix' = '" + System.currentTimeMillis() + "', " +
                        "'sink.properties.format' = 'json', " +
                        "'sink.buffer-count' = '4', " +
                        "'sink.buffer-size' = '4086'," +
                        "'sink.enable-2pc' = 'false' " + // 测试阶段可以关闭两阶段提交,方便测试
                        ");"
        );
        streamTableEnv.executeSql("insert into flink_doris_sink values (1, 1, 'jim', 7)");
    }
}
