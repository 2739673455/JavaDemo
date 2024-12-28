package com.atguigu.flink.timeandwindow;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;


/*
全窗口函数 - 全量聚合
* */

public class Flink05_ProcessWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 获取每条数据窗口信息
        env.fromSource(SourceUtil.getDataGeneratorSource(1), WatermarkStrategy.noWatermarks(), "ds")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTs();
                                    }
                                })
                )
                .keyBy(Event::getUser)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(
                        new ProcessWindowFunction<Event, Event, String, TimeWindow>() {
                            @Override
                            public void process(String key, ProcessWindowFunction<Event, Event, String, TimeWindow>.Context context, Iterable<Event> elements, Collector<Event> out) throws Exception {
                                // 对当前key所对应的窗口中数据进行处理
                                Long count = 0L;
                                for (Event element : elements) {
                                    count++;
                                }
                                // 获取窗口信息
                                long start = context.window().getStart();
                                long end = context.window().getEnd();

                                // 输出结果
                                out.collect(new Event(key, "窗口: " + start + " - " + end, count));
                            }
                        }
                )
                .print();

        env.execute();
    }
}
