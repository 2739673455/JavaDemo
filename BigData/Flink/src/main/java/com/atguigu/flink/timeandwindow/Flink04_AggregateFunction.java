package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;
/*
AggregateFunction: 聚合函数
 * */

public class Flink04_AggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 统计三秒内人均访问次数
        env.fromSource(SourceUtil.getDataGeneratorSource(6), WatermarkStrategy.noWatermarks(), "ds")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTs();
                                    }
                                })
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))
                .aggregate(
                        new AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>() {
                            // 初始化累加器对象，每个聚合任务只会调用一次
                            @Override
                            public Tuple2<Long, HashSet<String>> createAccumulator() {
                                return Tuple2.of(0L, new HashSet<>());
                            }

                            // 累加过程
                            @Override
                            public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> accumulator) {
                                accumulator.f1.add(event.getUser());
                                return Tuple2.of(accumulator.f0 + 1, accumulator.f1);
                            }

                            // 获取累加结果
                            @Override
                            public Double getResult(Tuple2<Long, HashSet<String>> accumulator) {
                                return (double) accumulator.f0 / accumulator.f1.size();
                            }

                            // 合并累加器，并将合并后的状态作为一个累加器返回
                            @Override
                            public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> accumulator1, Tuple2<Long, HashSet<String>> acc1) {
                                return null;
                            }
                        }
                )
                .print();

        env.execute();
    }
}
