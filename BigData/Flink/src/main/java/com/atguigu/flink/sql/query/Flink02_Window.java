package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.*;

/*
Flink提供的窗口:
    1. 分组窗口
        1. 滚动窗口
        2. 滑动窗口
        3. 会话窗口
    2. 窗口表值函数(TVF)
        1. 滚动窗口 Tumble Windows
        2. 滑动窗口 Hop Windows
        3. 会话窗口 Session Windows (will be supported)
        4. 累积窗口 Cumulate Windows
    3. over函数
* */

public class Flink02_Window {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<WaterSensor> ds = env.fromSource(SourceUtil.getWaterSensorGeneratorSource(2), WatermarkStrategy.noWatermarks(), "ds");
        Schema schema = Schema.newBuilder()
                .column("id", "string")
                .column("vc", "bigint")
                .column("ts", "bigint")
                .columnByExpression("process_time", "proctime()")
                .columnByExpression("event_time", "to_timestamp_ltz(ts,3)")
                .watermark("event_time", "event_time-interval '0' second")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("t1", table);

        //-TableAPI形式
        //---滚动窗口
        //-----计数滚动窗口
        TumbleWithSizeOnTimeWithAlias w1 = Tumble.over(rowInterval(4L)).on($("process_time")).as("w1");
        //table.window(w1)
        //        .groupBy($("w1"), $("id"))
        //        .select($("id"), $("vc").sum().as("sum_vc"))
        //        .execute().print();
        //-----时间滚动窗口
        //-------处理时间滚动窗口
        TumbleWithSizeOnTimeWithAlias w2 = Tumble.over(lit(5).seconds()).on($("process_time")).as("w2");
        //table.window(w2)
        //        .groupBy($("w2"), $("id"))
        //        .select($("id"), $("vc").sum().as("sum_vc"), $("w2").start().as("start"), $("w2").end().as("end"))
        //        .execute().print();
        //-------事件时间滚动窗口
        TumbleWithSizeOnTimeWithAlias w3 = Tumble.over(lit(5).seconds()).on($("event_time")).as("w3");
        //table.window(w3)
        //        .groupBy($("w3"), $("id"))
        //        .select($("id"), $("vc").sum().as("sum_vc"), $("w3").start().as("start"), $("w3").end().as("end"))
        //        .execute().print();
        //---滑动窗口
        //-----计数滑动窗口，第一个窗口必须满足窗口大小才开始滑动
        SlideWithSizeAndSlideOnTimeWithAlias w4 = Slide.over(rowInterval(3L)).every(rowInterval(2L)).on($("process_time")).as("w4");
        //table.window(w4)
        //        .groupBy($("w4"), $("id"))
        //        .select($("id"), $("vc").sum().as("sum_vc"))
        //        .execute().print();
        //-----时间滑动窗口
        //-------处理时间滑动窗口
        SlideWithSizeAndSlideOnTimeWithAlias w5 = Slide.over(lit(10).seconds()).every(lit(5).seconds()).on($("process_time")).as("w5");
        //table.window(w5)
        //        .groupBy($("w5"), $("id"))
        //        .select($("id"), $("vc").sum().as("sum_vc"), $("w5").start().as("start"), $("w5").end().as("end"))
        //        .execute().print();
        //-------事件时间滑动窗口
        SlideWithSizeAndSlideOnTimeWithAlias w6 = Slide.over(lit(10).seconds()).every(lit(5).seconds()).on($("event_time")).as("w6");
        //table.window(w6)
        //        .groupBy($("w6"), $("id"))
        //        .select($("id"), $("vc").sum().as("sum_vc"), $("w6").start().as("start"), $("w6").end().as("end"))
        //        .execute().print();
        //---会话窗口
        //-----处理时间会话窗口
        SessionWithGapOnTimeWithAlias w7 = Session.withGap(lit(5).seconds()).on($("process_time")).as("w7");
        //table.window(w7)
        //        .groupBy($("w7"), $("id"))
        //        .select($("id"), $("vc").sum().as("sum_vc"), $("w7").start().as("start"), $("w7").end().as("end"))
        //        .execute().print();
        //-----事件时间会话窗口
        SessionWithGapOnTimeWithAlias w8 = Session.withGap(lit(2).seconds()).on($("event_time")).as("w8");
        //table.window(w8)
        //        .groupBy($("w8"), $("id"))
        //        .select($("id"), $("vc").sum().as("sum_vc"), $("w8").start().as("start"), $("w8").end().as("end"))
        //        .execute().print();

        //-SQL形式
        //---滚动窗口
        //-----处理时间滚动窗口
        //streamTableEnv.sqlQuery(
        //        "select " +
        //                "id, " +
        //                "sum(vc) as sum_vc, " +
        //                "tumble_start(process_time, interval '5' second) as start_time, " +
        //                "tumble_end(process_time, interval '5' second) as end_time " +
        //                "from t1 group by tumble(process_time, interval '5' second), id"
        //).execute().print();
        //-----事件时间滚动窗口
        //streamTableEnv.sqlQuery(
        //        "select " +
        //                "id, " +
        //                "sum(vc) as sum_vc, " +
        //                "tumble_start(event_time, interval '5' second) as start_time, " +
        //                "tumble_end(event_time, interval '5' second) as end_time " +
        //                "from t1 group by tumble(event_time, interval '5' second), id"
        //).execute().print();
        //---滑动窗口
        //-----处理时间滑动窗口
        //streamTableEnv.sqlQuery(
        //        "select " +
        //                "id, " +
        //                "sum(vc) as sum_vc, " +
        //                "hop_start(process_time, interval '2' second, interval '5' second) as start_time, " +
        //                "hop_end(process_time, interval '2' second, interval '5' second) as end_time " +
        //                "from t1 group by hop(process_time, interval '2' second, interval '5' second), id"
        //).execute().print();
        //-----事件时间滑动窗口
        //streamTableEnv.sqlQuery(
        //        "select " +
        //                "id, " +
        //                "sum(vc) as sum_vc, " +
        //                "hop_start(event_time, interval '2' second, interval '5' second) as start_time, " +
        //                "hop_end(event_time, interval '2' second, interval '5' second) as end_time " +
        //                "from t1 group by hop(event_time, interval '2' second, interval '5' second), id"
        //).execute().print();
        //---会话窗口
        //-----处理时间会话窗口
        //streamTableEnv.sqlQuery(
        //        "select " +
        //                "id, " +
        //                "sum(vc) as sum_vc, " +
        //                "session_start(process_time, interval '3' second) as start_time, " +
        //                "session_end(process_time, interval '3' second) as end_time " +
        //                "from t1 group by session(process_time, interval '3' second), id"
        //).execute().print();
        //-----事件时间会话窗口
        //streamTableEnv.sqlQuery(
        //        "select " +
        //                "id, " +
        //                "sum(vc) as sum_vc, " +
        //                "session_start(event_time, interval '3' second) as start_time, " +
        //                "session_end(event_time, interval '3' second) as end_time " +
        //                "from t1 group by session(event_time, interval '3' second), id"
        //).execute().print();

        //-WindowTVF写法
        //---滚动窗口
        //streamTableEnv.sqlQuery(
        //        "select id, sum(vc) as sum_vc, window_start, window_end " +
        //                "from table (tumble(table t1, descriptor(process_time), interval '5' second)) " +
        //                "group by window_start, window_end, id"
        //).execute().print();
        //---滑动窗口
        //streamTableEnv.sqlQuery(
        //        "select id, sum(vc) as sum_vc, window_start, window_end " +
        //                "from table (hop(table t1, descriptor(event_time), interval '5' second, interval '10' second)) " +
        //                "group by window_start, window_end, id"
        //).execute().print();
        //---累积窗口
        //streamTableEnv.sqlQuery(
        //        "select sum(vc) as sum_vc, window_start, window_end " +
        //                "from table (cumulate(table t1, descriptor(event_time), interval '2' second, interval '10' second)) " +
        //                "group by window_start, window_end"
        //).execute().print();

        //-over函数: following不能为下无边界
        //---TableAPI形式
        //Table table = input
        //    .window([OverWindow w].as("w"))           // define over window with alias w
        //    .select($("a"), $("b").sum().over($("w")), $("c").min().over($("w"))); // aggregate over the over window w
        //-----基于行
        //-------上无边界到当前行
        //OverWindowPartitionedOrderedPreceding overWindow1 = Over.partitionBy($("id")).orderBy($("process_time")).preceding(UNBOUNDED_ROW).following(CURRENT_ROW);
        //table.window(overWindow1.as("w"))
        //        .select($("id"), $("vc"), $("ts"), $("vc").sum().over($("w")).as("sum_vc"))
        //        .execute().print();
        //-------上2行到当前行
        //OverWindowPartitionedOrderedPreceding overWindow2 = Over.partitionBy($("id")).orderBy($("process_time")).preceding(rowInterval(2L)).following(CURRENT_ROW);
        //table.window(overWindow2.as("w"))
        //        .select($("id"), $("vc"), $("ts"), $("vc").sum().over($("w")).as("sum_vc"))
        //        .execute().print();
        //-----基于时间
        //-------上无边界到当前时间
        //OverWindowPartitionedOrderedPreceding overWindow3 = Over.partitionBy($("id")).orderBy($("event_time")).preceding(UNBOUNDED_RANGE).following(CURRENT_RANGE);
        //table.window(overWindow3.as("w"))
        //        .select($("id"), $("vc"), $("process_time"), $("event_time"), $("vc").sum().over($("w")).as("sum_vc"))
        //        .execute().print();
        //-------上2秒到当前时间
        //OverWindowPartitionedOrderedPreceding overWindow4 = Over.partitionBy($("id")).orderBy($("event_time")).preceding(lit(2).seconds()).following(CURRENT_RANGE);
        //table.window(overWindow4.as("w"))
        //        .select($("id"), $("vc"), $("process_time"), $("event_time"), $("vc").sum().over($("w")).as("sum_vc"))
        //        .execute().print();

        //---SQL形式
        //-----基于行
        //streamTableEnv.sqlQuery(
        //        "select id, vc, process_time, event_time, " +
        //                "sum(vc) over(partition by id order by process_time rows between unbounded preceding and current row) as sum_vc " +
        //                "from t1 "
        //).execute().print();
        //-----基于时间
        //streamTableEnv.sqlQuery(
        //        "select id, vc, process_time, event_time, " +
        //                "sum(vc) over(partition by id order by event_time range between interval '2' second preceding and current row) as sum_vc " +
        //                "from t1 "
        //).execute().print();
    }
}
