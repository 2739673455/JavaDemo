package com.atguigui.gmall.realtime.test;


import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test01_FlinkCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> mySqlStrDS = env.fromSource(
                MySqlSource.<String>builder()
                        .hostname("localhost")
                        .port(3306)
                        .databaseList("db1")
                        .tableList("db1" + "." + "event_table1")
                        .username("root")
                        .password("123321")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .startupOptions(StartupOptions.initial())
                        .build(),
                WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
        mySqlStrDS.print();
        env.execute();
    }
}
