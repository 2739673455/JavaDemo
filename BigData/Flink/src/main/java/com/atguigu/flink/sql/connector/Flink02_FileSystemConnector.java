package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink02_FileSystemConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        streamTableEnv.executeSql(
                "create table t_source(" +
                        "id string, " +
                        "vc int, " +
                        "ts bigint, " +
                        //"`file.path` string not null metadata, " +
                        //"`file.name` string not null metadata, " +
                        "`file.size` bigint not null metadata, " +
                        "`file.modification-time` timestamp_ltz(3) not null metadata" +
                        ") " +
                        "with(" +
                        "'connector'='filesystem', " +
                        "'path'='D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\input\\ws.txt', " +
                        "'format'='csv', " +
                        "'csv.field-delimiter'=' '" +
                        ")"
        );
        streamTableEnv.executeSql(
                "create table t_sink(" +
                        "id string, " +
                        "vc int, " +
                        "ts bigint, " +
                        //"`file_path` string, " +
                        //"`file_name` string, " +
                        "`file_size` bigint, " +
                        "`file_modification-time` timestamp_ltz(3)" +
                        ") " +
                        "with(" +
                        "'connector'='filesystem', " +
                        "'path'='D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\output', " +
                        "'format'='json'" +
                        ")"
        );
        Table result = streamTableEnv.sqlQuery("select * from t_source");
        result.execute().print();
        result.executeInsert("t_sink");
    }
}
