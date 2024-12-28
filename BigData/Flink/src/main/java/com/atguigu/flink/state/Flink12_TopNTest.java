package com.atguigu.flink.state;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;

/*
案例需求:
    网站中一个非常经典的例子，就是实时统计一段时间内的热门url
    例如，需要统计最近10秒钟内最热门的两个url链接，并且每5秒钟更新一次
    我们知道，这可以用一个滑动窗口来实现，而“热门度”一般可以直接用访问量来表示
    于是就需要开滑动窗口收集url的访问数据，按照不同的url进行统计，而后汇总排序并最终输出前两名
方式二:
    聚合的过程使用增量聚合
    窗口信息的获取使用全窗口函数
* */
public class Flink12_TopNTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(1000L);
        SingleOutputStreamOperator<UrlCount> ds1 = env.fromSource(SourceUtil.getDataGeneratorSource(4), WatermarkStrategy.noWatermarks(), "ds")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long l) {
                                        return event.getTs();
                                    }
                                })
                )
                .map(o -> new UrlCount(0L, 0L, o.getUrl(), 1L, o.getTs()))
                .returns(Types.POJO(UrlCount.class))
                .keyBy(UrlCount::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(4L)))
                // 按url分组聚合，求出点击次数，获取窗口开始结束时间
                .reduce(
                        (result, in) -> {
                            result.setCount(result.getCount() + in.getCount());
                            result.setTs(in.getTs());
                            return result;
                        },
                        new ProcessWindowFunction<UrlCount, UrlCount, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<UrlCount, UrlCount, String, TimeWindow>.Context context, Iterable<UrlCount> iterable, Collector<UrlCount> collector) {
                                UrlCount urlCount = iterable.iterator().next();
                                urlCount.setStartTime(context.window().getStart());
                                urlCount.setEndTime(context.window().getEnd());
                                collector.collect(urlCount);
                            }
                        }
                );
        //ds1.print();
        ds1
                .keyBy(UrlCount::getEndTime)
                .window(TumblingEventTimeWindows.of(Time.seconds(4L)))
                .process(
                        new ProcessWindowFunction<UrlCount, UrlCount, Long, TimeWindow>() {
                            @Override
                            public void process(Long l, ProcessWindowFunction<UrlCount, UrlCount, Long, TimeWindow>.Context context, Iterable<UrlCount> iterable, Collector<UrlCount> collector) {
                                ArrayList<UrlCount> urlCountList = new ArrayList<>();
                                for (UrlCount urlCount : iterable) {
                                    urlCountList.add(urlCount);
                                }
                                urlCountList.sort((o1, o2) -> -Math.toIntExact(o1.getCount() - o2.getCount()));
                                for (UrlCount urlCount : urlCountList) {
                                    collector.collect(urlCount);
                                    System.out.println(urlCount);
                                }
                                System.out.println(context.window().getStart() + "---------------" + context.window().getEnd());
                            }
                        }
                );
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
            return startTime + "\t" + endTime + "\t" + url + ":" + count + "  \tts=" + ts;
        }
    }
}
