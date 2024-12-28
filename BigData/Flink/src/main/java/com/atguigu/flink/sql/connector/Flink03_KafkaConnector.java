package com.atguigu.flink.sql.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink03_KafkaConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 读取
        streamTableEnv.executeSql(
                "create table t_source (id string, vc bigint, ts bigint," +
                        "`partition` INT NOT NULL metadata," +
                        "`offset` BIGINT NOT NULL metadata," +
                        "`timestamp` TIMESTAMP_LTZ(3) NOT NULL metadata" +
                        ") " +
                        "with(" +
                        "'connector'='kafka'," +
                        "'topic'='topicA'," +
                        "'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                        "'properties.group.id'='flink'," +
                        "'scan.startup.mode'='earliest-offset'," +
                        "'format'='csv'" +
                        ")"
        );
        // 写入
        streamTableEnv.executeSql(
                "create table t_sink (id string, vc bigint, ts bigint," +
                        "prt int," +
                        "oft bigint," +
                        "tmsp TIMESTAMP_LTZ(3)" +
                        ") " +
                        "with(" +
                        "'connector'='kafka'," +
                        "'topic'='topicB'," +
                        "'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                        "'sink.delivery-guarantee'='at-least-once'," +
                        //"'sink.delivery-guarantee'='exactly-once'," +
                        //"'sink.transactional-id-prefix'='flink-'" + System.currentTimeMillis() + "," +
                        "'format'='csv'" +
                        ")"
        );
        Table appendTable = streamTableEnv.sqlQuery("select * from t_source");
        appendTable.executeInsert("t_sink");
        // 使用upsert-kafka写入
        streamTableEnv.executeSql(
                "create table t_sink (" +
                        "id string, sum_vc bigint, primary key (id) not enforced" +
                        ") " +
                        "with(" +
                        "'connector'='upsert-kafka'," +
                        "'topic'='topicB'," +
                        "'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092'," +
                        "'key.format'='csv'," +
                        "'value.format'='csv'" +
                        ")"
        );
        Table updateTable = streamTableEnv.sqlQuery("select id,sum(vc) as sum_vc from t_source group by id");
        updateTable.executeInsert("t_sink");
    }
}
