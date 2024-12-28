package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.UserLoginBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;

public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(10024, 4, "dws_user_user_login_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        kafkaStrDS.map(JSON::parseObject)
                .filter(o -> Arrays.asList("login", null).contains(o.getJSONObject("page").getString("last_page_id")) && o.getJSONObject("common").getString("uid") != null)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofMillis(0))
                        .withTimestampAssigner((o, l) -> o.getLong("ts"))
                )
                .keyBy(o -> o.getJSONObject("common").getString("uid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
                            ValueState<String> backUser;
                            ValueState<String> newUser;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<String> backUserStateDescriptor = new ValueStateDescriptor<>("bucketUser", String.class);
                                ValueStateDescriptor<String> newUserStateDescriptor = new ValueStateDescriptor<>("newUser", String.class);
                                backUser = getRuntimeContext().getState(backUserStateDescriptor);
                                newUser = getRuntimeContext().getState(newUserStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context context, Collector<UserLoginBean> collector) throws Exception {
                                Long ts = in.getLong("ts");
                                String date = DateFormatUtil.tsToDate(ts);
                                String backUserStateValue = backUser.value();
                                String newUserStateValue = newUser.value();
                                boolean collectFlag = false;
                                UserLoginBean userLoginBean = new UserLoginBean("", "", date, 0L, 0L, ts);
                                if (newUserStateValue == null || !newUserStateValue.equals(date)) {
                                    collectFlag = true;
                                    userLoginBean.setUuCt(1L);
                                }
                                newUser.update(date);
                                if (backUserStateValue != null && DateFormatUtil.dateToTs(date) - DateFormatUtil.dateToTs(backUserStateValue) > 1000 * 60 * 60 * 24 * 7) {
                                    collectFlag = true;
                                    userLoginBean.setBackCt(1L);
                                    backUser.update(date);
                                }
                                backUser.update(date);
                                if (collectFlag)
                                    collector.collect(userLoginBean);
                            }
                        }
                )
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(
                        new ReduceFunction<UserLoginBean>() {
                            @Override
                            public UserLoginBean reduce(UserLoginBean result, UserLoginBean in) {
                                result.setUuCt(result.getUuCt() + in.getUuCt());
                                result.setBackCt(result.getBackCt() + in.getBackCt());
                                return result;
                            }
                        },
                        new ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>.Context context, Iterable<UserLoginBean> iterable, Collector<UserLoginBean> collector) {
                                UserLoginBean next = iterable.iterator().next();
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                next.setStt(start);
                                next.setEdt(end);
                                collector.collect(next);
                            }
                        }
                )
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_user_user_login_window"));
    }
}
