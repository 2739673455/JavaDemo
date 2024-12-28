package com.atguigu.flink.sql.time;


import com.atguigu.flink.pojo.WaterSensor;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/*
建表指定时间字段:
    1. 流转表时指定
    2. 连接器指定
* */
public class Flink01_TimeAttribute {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        SingleOutputStreamOperator<WaterSensor> ds = env.fromSource(SourceUtil.getWaterSensorGeneratorSource(3), WatermarkStrategy.noWatermarks(), "ds")
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                        .withTimestampAssigner((o, l) -> o.getTs()));
        // 流转表指定时间字段
        Schema schema = Schema.newBuilder()
                .column("id", "string")
                .column("vc", "bigint")
                .column("ts", "bigint")
                .columnByExpression("process_time", "proctime()") // 处理时间字段
                .columnByExpression("event_time", "to_timestamp_ltz(ts,3)") // 事件时间字段
                //.watermark("eventTime", "source_watermark()") // 水位线: 直接沿用流中的水位线
                .watermark("event_time", "event_time-interval '0.5' second") // 水位线: 重新指定水位线
                .build();
        //Table table = streamTableEnv.fromDataStream(ds, schema);

        // 连接器表指定时间字段
        streamTableEnv.executeSql(
                "create table t_source (" +
                        "id string, " +
                        "vc bigint, " +
                        "ts bigint, " +
                        "`timestamp` TIMESTAMP_LTZ(3) NOT NULL metadata, " +
                        "process_time as proctime(), " + // 处理时间字段
                        "event_time as to_timestamp_ltz(ts,3), " + // 事件时间字段
                        "watermark for event_time as event_time-interval '1' second" + // 水位线
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
        Table table = streamTableEnv.from("t_source");
        table.printSchema();
    }
}
