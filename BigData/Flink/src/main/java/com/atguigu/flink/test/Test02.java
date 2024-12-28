package com.atguigu.flink.test;

import com.atguigu.flink.pojo.UrlCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;

/*
考试题: 从Kafka TopicA 读取Event数据, 求每10秒点击次数最高的前两个url, 将统计结果写出到Mysql t_url_top2表中。
要求: 使用增量函数、全窗口函数、状态、定时器来完成 , 结果中需要包含窗口信息
* */
public class Test02 {
    public static void main(String[] args) throws Exception {
        // kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("topicA")
                .setGroupId("flink")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .build();
        // jdbc sink
        SinkFunction<UrlCount> jdbcSink = JdbcSink.sink(
                "insert into t_url_top2 (url,count,window_start,window_end) values(?,?,?,?)",
                new JdbcStatementBuilder<UrlCount>() {
                    @Override
                    public void accept(PreparedStatement p, UrlCount urlCount) throws SQLException {
                        p.setString(1, urlCount.getUrl());
                        p.setLong(2, urlCount.getCount());
                        p.setLong(3, urlCount.getStartTime());
                        p.setLong(4, urlCount.getEndTime());
                    }
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName("com.mysql.cj.jdbc.Driver")
                        .withUrl("jdbc:mysql://localhost:3306/db1")
                        .withUsername("root")
                        .withPassword("123321")
                        .build()
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启检查点
        env.enableCheckpointing(2000L);
        env.setParallelism(1);
        SingleOutputStreamOperator<UrlCount> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "ds")
                .map(o -> new UrlCount(0L, 0L, o.split(" ")[1], 1L, System.currentTimeMillis()))
                .returns(Types.POJO(UrlCount.class))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<UrlCount>forBoundedOutOfOrderness(Duration.ofMillis(0L)).withTimestampAssigner(
                                new SerializableTimestampAssigner<UrlCount>() {
                                    @Override
                                    public long extractTimestamp(UrlCount o, long l) {
                                        return o.getTs();
                                    }
                                }
                        )
                )
                .keyBy(UrlCount::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(
                        new ReduceFunction<UrlCount>() {
                            @Override
                            public UrlCount reduce(UrlCount result, UrlCount in) throws Exception {
                                result.setCount(result.getCount() + in.getCount());
                                return result;
                            }
                        },
                        new ProcessWindowFunction<UrlCount, UrlCount, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<UrlCount, UrlCount, String, TimeWindow>.Context context, Iterable<UrlCount> iterable, Collector<UrlCount> collector) throws Exception {
                                UrlCount urlCount = iterable.iterator().next();
                                urlCount.setStartTime(context.window().getStart());
                                urlCount.setEndTime(context.window().getEnd());
                                collector.collect(urlCount);
                            }
                        }
                )
                .keyBy(UrlCount::getEndTime)
                .process(
                        new KeyedProcessFunction<Long, UrlCount, UrlCount>() {
                            private ListState<UrlCount> list;

                            @Override
                            public void open(Configuration parameters) {
                                ListStateDescriptor<UrlCount> listStateDescriptor = new ListStateDescriptor<>("listState", Types.POJO(UrlCount.class));
                                list = getRuntimeContext().getListState(listStateDescriptor);
                            }

                            @Override
                            public void processElement(UrlCount urlCount, KeyedProcessFunction<Long, UrlCount, UrlCount>.Context context, Collector<UrlCount> collector) throws Exception {
                                list.add(urlCount);
                                context.timerService().registerEventTimeTimer(context.getCurrentKey());
                            }

                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlCount, UrlCount>.OnTimerContext context, Collector<UrlCount> out) throws Exception {
                                ArrayList<UrlCount> urlCounts = new ArrayList<>();
                                for (UrlCount urlCount : list.get()) {
                                    urlCounts.add(urlCount);
                                }
                                urlCounts.sort((o1, o2) -> -Long.compare(o1.getCount(), o2.getCount()));
                                for (int i = 0; i < Math.min(2, urlCounts.size()); ++i) {
                                    out.collect(urlCounts.get(i));
                                }
                                System.out.println("----------");
                            }
                        }
                );
        ds.print();
        ds.addSink(jdbcSink);

        env.execute();
    }
}
