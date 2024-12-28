package com.atguigu.flink.state;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*
案例需求:
    网站中一个非常经典的例子，就是实时统计一段时间内的热门url
    例如，需要统计最近10秒钟内最热门的两个url链接，并且每5秒钟更新一次
    我们知道，这可以用一个滑动窗口来实现，而“热门度”一般可以直接用访问量来表示
    于是就需要开滑动窗口收集url的访问数据，按照不同的url进行统计，而后汇总排序并最终输出前两名
方式一: 使用全窗口函数实现
* */
public class Flink11_TopNProcessAllWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(SourceUtil.getDataGeneratorSource(3), WatermarkStrategy.noWatermarks(), "ds")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                                .withTimestampAssigner((e, l) -> e.getTs())
                )
                .map(o -> Tuple2.of(o.getUrl(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10L), Time.seconds(5L)))
                .process(
                        new ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<Tuple2<String, Long>, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> iterable, Collector<String> collector) throws Exception {
                                HashMap<String, Long> map = new HashMap<>();
                                for (Tuple2<String, Long> t : iterable) {
                                    map.put(t.f0, map.getOrDefault(t.f0, 0L) + 1L);
                                }
                                // 获取窗口信息
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                // 排序
                                List<Map.Entry<String, Long>> list = new ArrayList<>(map.entrySet());
                                list.sort((o1, o2) -> -Math.toIntExact(o1.getValue() - o2.getValue()));
                                for (int i = 0; i < Math.min(2, list.size()); ++i) {
                                    collector.collect(start + "\t" + end + "\t" + list.get(i).toString());
                                }
                                System.out.println("---------------");
                            }
                        }
                )
                .print();

        env.execute();
    }
}
