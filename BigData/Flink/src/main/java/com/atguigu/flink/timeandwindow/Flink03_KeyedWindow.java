package com.atguigu.flink.timeandwindow;

/*
使用窗口的关注点:
1. 明确是按键分区窗口(KeyedStream)，还是非按键分区(普通流)
2. 窗口分配器，名曲额使用哪种类型的窗口
3. 窗口函数，明确窗口中的数据如何进行计算

按键分区窗口，每个分区都有一个窗口
* */

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class Flink03_KeyedWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(SourceUtil.getDataGeneratorSource(1), WatermarkStrategy.noWatermarks(), "Source")
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                        .withTimestampAssigner((event, l) -> event.getTs())
                )
                .map(o -> {
                    System.out.println(o);
                    return Tuple2.of(o.getUser(), 1L);
                })
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(o -> o.f0)

                // 非按键分区窗口
                // 计数窗口
                //.countWindowAll(3) // 计数滚动窗口，每 size 个数据滚动一次
                //.countWindowAll(3, 2) // 计数滑动窗口，每 slide 个数据滑动一次，滑动后数据需剩余 size-slide 个
                // 计时窗口
                //.windowAll(
                //TumblingProcessingTimeWindows.of(Time.seconds(5L), Time.seconds(-1L))) // 处理时间滚动窗口，第二个参数为时区偏移
                //TumblingEventTimeWindows.of(Time.seconds(5L))) // 事件时间滚动窗口
                //SlidingProcessingTimeWindows.of(Time.seconds(10L), Time.seconds(5L))) // 处理时间滑动窗口
                //SlidingEventTimeWindows.of(Time.seconds(10L), Time.seconds(5L))) // 事件时间滑动窗口
                //ProcessingTimeSessionWindows.withGap(Time.seconds(5L))) // 处理时间会话窗口
                //EventTimeSessionWindows.withGap(Time.seconds(5L))) // 事件时间会话窗口
                //GlobalWindows.create()) // 全局窗口

                // 按键分区窗口，每个分区都有一个窗口
                // 计数窗口
                //.countWindow(3) // 计数滚动窗口，每 size 个数据滚动一次
                //.countWindow(4, 1) // 计数滑动窗口，每 slide 个数据滑动一次，滑动后数据需剩余 size-slide 个
                // 计时窗口
                .window(
                        TumblingProcessingTimeWindows.of(Time.seconds(5L))) // 处理时间滚动窗口，第二个参数为时区偏移
                //TumblingEventTimeWindows.of(Time.seconds(5L))) // 事件时间滚动窗口
                //SlidingProcessingTimeWindows.of(Time.seconds(10L), Time.seconds(5L))) // 处理时间滑动窗口
                //SlidingEventTimeWindows.of(Time.seconds(10L), Time.seconds(5L))) // 事件时间滑动窗口
                //ProcessingTimeSessionWindows.withGap(Time.seconds(5L))) // 处理时间会话窗口
                //EventTimeSessionWindows.withGap(Time.seconds(5L))) // 事件时间会话窗口
                //GlobalWindows.create()) // 全局窗口

                .sum(1)
                .print();

        env.execute();
    }
}
