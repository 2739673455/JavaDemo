package com.atguigu.flink.timeandwindow;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;

/*
迟到数据:
    1. 推迟水位线
    2. 窗口延迟关闭 - 保证少量的迟到更久的数据参与计算，在迟到数据到达时窗口再计算一次
    3. 侧输出流 - 窗口已关闭，一些少数迟到数据到达后，Flink会直接将该数据输出到侧输出流中

统计每5秒内每个url的点击次数，结果包含窗口信息，将窗口统计好的结果输出到mysql中
* */

public class Flink07_FixLateData {
    // 统计每5秒内每个url的点击次数，结果包含窗口信息，将窗口统计好的结果输出到mysql中
    public static void main(String[] args) throws Exception {

        OutputTag<UrlCount> lateData = new OutputTag<>("lateDate", Types.POJO(UrlCount.class));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(2000L);

        // 从端口读入数据 "user url timestamp"
        // 转换为UrlCount(startTime, endTime, url, count, ts)
        // 输出到MySQL "window_start, window_end, url, count"
        SingleOutputStreamOperator<UrlCount> ds = env.socketTextStream("localhost", 9999)
                .map(line -> new UrlCount(0L, 0L, line.split(" ")[1], 1L, Long.parseLong(line.split(" ")[2])))
                .returns(Types.POJO(UrlCount.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UrlCount>forBoundedOutOfOrderness(Duration.ofSeconds(2)) // 推迟水位线
                                .withTimestampAssigner((o, l) -> o.getTs())
                )
                .keyBy(UrlCount::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .allowedLateness(Time.seconds(5)) // 窗口延迟关闭
                .sideOutputLateData(lateData) // 迟到数据放入侧输出流
                .reduce(
                        new ReduceFunction<UrlCount>() {
                            @Override
                            public UrlCount reduce(UrlCount result, UrlCount in) throws Exception {
                                return new UrlCount(0L, 0L, in.getUrl(), result.getCount() + in.getCount(), in.getTs());
                            }
                        },
                        new ProcessWindowFunction<UrlCount, UrlCount, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<UrlCount, UrlCount, String, TimeWindow>.Context context, Iterable<UrlCount> iterable, Collector<UrlCount> collector) throws Exception {
                                UrlCount o = iterable.iterator().next();
                                o.setStartTime(context.window().getStart());
                                o.setEndTime(context.window().getEnd());
                                collector.collect(o);
                            }
                        }
                );

        // 主流 MySQL Sink
        SinkFunction<UrlCount> mainSink = JdbcSink.sink(
                "replace into url_view_count(window_start,window_end,url,count) values(?,?,?,?)",
                new JdbcStatementBuilder<UrlCount>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, UrlCount o) throws SQLException {
                        preparedStatement.setLong(1, o.getStartTime());
                        preparedStatement.setLong(2, o.getEndTime());
                        preparedStatement.setString(3, o.getUrl());
                        preparedStatement.setLong(4, o.getCount());
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/db1")
                        .withUsername("root")
                        .withPassword("123321")
                        .build()
        );
        ds.addSink(mainSink);
        ds.print("mainDS");

        // 获取迟到流并计算窗口开始时间
        SingleOutputStreamOperator<UrlCount> lateDs = ds.getSideOutput(lateData)
                .map(o -> {
                            long startTime = o.getTs() - o.getTs() % 5000;
                            o.setStartTime(startTime);
                            o.setEndTime(startTime + 5000);
                            return o;
                        }
                );
        // 迟到流 MySQL Sink
        SinkFunction<UrlCount> lateSink = JdbcSink.sink(
                "insert into url_view_count(window_start,window_end,url,count) values(?,?,?,?) on duplicate key update count=count+values(count)",
                new JdbcStatementBuilder<UrlCount>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, UrlCount o) throws SQLException {
                        preparedStatement.setLong(1, o.getStartTime());
                        preparedStatement.setLong(2, o.getEndTime());
                        preparedStatement.setString(3, o.getUrl());
                        preparedStatement.setLong(4, o.getCount());
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/db1")
                        .withUsername("root")
                        .withPassword("123321")
                        .build()
        );
        lateDs.addSink(lateSink);
        lateDs.print("lateDs");

        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UrlCount {
        private Long startTime;
        private Long endTime;
        private String url;
        private Long count;
        private Long ts;

        @Override
        public String toString() {
            return "startTime=" + startTime +
                    "\tendTime=" + endTime +
                    "\turl=" + url +
                    "\tcount=" + count +
                    "\tts=" + ts;
        }
    }
}