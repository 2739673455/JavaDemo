package com.atguigu.gmall.realtime.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test01_Sql_Join {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        // 从Kafka读取数据
        streamTableEnv.executeSql(
                "create table emp ( " +
                        "empId string, " +
                        "empName string, " +
                        "deptId string, " +
                        "process_time as proctime() " +
                        ") " +
                        "with ( " +
                        "'connector'='kafka', " +
                        "'properties.bootstrap.servers'='hadoop102:9092', " +
                        "'properties.group.id'='testGroup', " +
                        "'topic'='topicA', " +
                        "'scan.startup.mode'='latest-offset', " +
                        "'format'='csv' " +
                        ")"
        );

        // 从HBase读取数据
        streamTableEnv.executeSql(
                "create table hTable ( " +
                        "deptId string, " +
                        "info row<deptName string>, " +
                        "primary key(deptId) not enforced " +
                        ") " +
                        "with ( " +
                        "'connector'='hbase-2.2', " +
                        "'table-name'='t1', " +
                        "'zookeeper.quorum'='hadoop102:2181,hadoop103:2181,hadoop104:2181', " +
                        "'lookup.async'='true', " +
                        "'lookup.cache'='PARTIAL', " +
                        "'lookup.partial-cache.max-rows'='200', " +
                        "'lookup.partial-cache.expire-after-write'='10 second', " +
                        "'lookup.partial-cache.expire-after-access'='10 second' " +
                        ")"
        );

        // 关联数据
        Table table = streamTableEnv.sqlQuery(
                "select emp.empId, emp.empName, hTable.deptId, hTable.deptName " +
                        "from emp " +
                        "left join hTable " +
                        "for system_time as of emp.process_time " +
                        "on emp.deptId=hTable.deptId "
        );

        // 将数据写入Kafka
        streamTableEnv.executeSql(
                "create table kafka_sink ( " +
                        "empId string, " +
                        "empName string, " +
                        "deptId string, " +
                        "deptName string, " +
                        "primary key(empId) not enforced " +
                        ") " +
                        "with ( " +
                        "'connector'='upsert-kafka', " +
                        "'properties.bootstrap.servers'='hadoop102:9092,hadoop103:9092,hadoop104:9092', " +
                        "'topic'='topicB', " +
                        "'key.format'='csv', " +
                        "'value.format'='csv' " +
                        ") "
        );
        table.executeInsert("kafka_sink");
    }
}