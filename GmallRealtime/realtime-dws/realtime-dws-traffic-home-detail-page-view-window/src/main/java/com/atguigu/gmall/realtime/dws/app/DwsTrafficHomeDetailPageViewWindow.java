package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficHomeDetailPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;

public class DwsTrafficHomeDetailPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficHomeDetailPageViewWindow().start(10023, 4, "dws_traffic_home_detail_page_view_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        kafkaStrDS.map(JSON::parseObject)
                .filter(o -> Arrays.asList("home", "good_detail").contains(o.getJSONObject("page").getString("page_id")))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                                .withTimestampAssigner((o, l) -> (o.getLong("ts")))
                )
                .keyBy(o -> o.getJSONObject("common").getString("mid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                            ValueState<String> homeState;
                            ValueState<String> detailState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> homeStateDescriptor = new ValueStateDescriptor<>("home", String.class);
                                ValueStateDescriptor<String> detailStateDescriptor = new ValueStateDescriptor<>("detail", String.class);
                                homeStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                detailStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                homeState = getRuntimeContext().getState(homeStateDescriptor);
                                detailState = getRuntimeContext().getState(detailStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context context, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                                Long ts = in.getLong("ts");
                                String pageId = in.getJSONObject("page").getString("page_id");
                                String date = DateFormatUtil.tsToDate(ts);
                                String homeStateValue = homeState.value();
                                String detailStateValue = detailState.value();
                                TrafficHomeDetailPageViewBean trafficHomeDetailPageView = new TrafficHomeDetailPageViewBean("", "", date, 0L, 0L, ts);
                                if ("home".equals(pageId) && (homeStateValue == null || homeStateValue.isEmpty() || !date.equals(homeStateValue))) {
                                    trafficHomeDetailPageView.setHomeUvCt(1L);
                                    collector.collect(trafficHomeDetailPageView);
                                    homeState.update(date);
                                } else if ("good_detail".equals(pageId) && (detailStateValue == null || detailStateValue.isEmpty() || !date.equals(detailStateValue))) {
                                    trafficHomeDetailPageView.setGoodDetailUvCt(1L);
                                    collector.collect(trafficHomeDetailPageView);
                                    detailState.update(date);
                                }
                            }
                        }
                )
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                            @Override
                            public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean result, TrafficHomeDetailPageViewBean in) throws Exception {
                                result.setHomeUvCt(result.getHomeUvCt() + in.getHomeUvCt());
                                result.setGoodDetailUvCt(result.getGoodDetailUvCt() + in.getGoodDetailUvCt());
                                return result;
                            }
                        },
                        new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context, Iterable<TrafficHomeDetailPageViewBean> iterable, Collector<TrafficHomeDetailPageViewBean> collector) throws Exception {
                                TrafficHomeDetailPageViewBean next = iterable.iterator().next();
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                next.setStt(start);
                                next.setEdt(end);
                                collector.collect(next);
                            }
                        }
                )
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window"));
    }
}
