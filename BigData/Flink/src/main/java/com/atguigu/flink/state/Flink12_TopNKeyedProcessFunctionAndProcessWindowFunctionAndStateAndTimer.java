package com.atguigu.flink.state;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
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
public class Flink12_TopNKeyedProcessFunctionAndProcessWindowFunctionAndStateAndTimer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(SourceUtil.getDataGeneratorSource(3), WatermarkStrategy.noWatermarks(), "ds")
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                                .withTimestampAssigner((e, l) -> e.getTs())
                )
                .map(o -> new UrlCount(0L, 0L, o.getUrl(), 1L, o.getTs()))
                .returns(Types.POJO(UrlCount.class))
                .keyBy(UrlCount::getUrl)
                .window(SlidingEventTimeWindows.of(Time.seconds(10L), Time.seconds(5L)))
                // 按url分组聚合，求出点击次数，获取窗口开始结束时间
                .reduce(
                        (result, in) -> {
                            result.setCount(result.getCount() + in.getCount());
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
                )
                .keyBy(UrlCount::getEndTime)
                // 按窗口结束时间分组，排序求出点击量前二的url
                .process(
                        new KeyedProcessFunction<Long, UrlCount, UrlCount>() {
                            private ListState<UrlCount> state;

                            @Override
                            public void open(Configuration parameters) {
                                ListStateDescriptor<UrlCount> stateDescriptor = new ListStateDescriptor<>("state", Types.POJO(UrlCount.class));
                                state = getRuntimeContext().getListState(stateDescriptor);
                            }

                            @Override
                            public void processElement(UrlCount urlCount, KeyedProcessFunction<Long, UrlCount, UrlCount>.Context context, Collector<UrlCount> collector) throws Exception {
                                // 将urlCount添加进状态
                                state.add(urlCount);
                                // 使用窗口结束时间注册定时任务
                                context.timerService().registerEventTimeTimer(context.getCurrentKey());
                            }

                            // 定时任务内容
                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<Long, UrlCount, UrlCount>.OnTimerContext ctx, Collector<UrlCount> out) throws Exception {
                                ArrayList<UrlCount> urlCountList = new ArrayList<>();
                                // 从状态中获取urlCount放入ArrayList准备排序
                                for (UrlCount urlCount : state.get()) {
                                    urlCountList.add(urlCount);
                                }
                                // 排序
                                urlCountList.sort((o1, o2) -> -Math.toIntExact(o1.getCount() - o2.getCount()));
                                // 收集前二
                                for (int i = 0; i < Math.min(2, urlCountList.size()); ++i) {
                                    out.collect(urlCountList.get(i));
                                }
                                System.out.println("---------------");
                                // 使用后清空状态
                                state.clear();
                            }
                        }
                )
                .print();
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
