package com.atguigu.edu.realtime.dws.trade.total.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;

public class DwsTradeTotalWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeTotalWindow().start(10115, 4, "dws_trade_total_window", "dwd_trade_order_detail");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> result = kafkaStrDS
                .filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .keyBy(o -> o.getString("id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            ValueState<BigDecimal> jsonState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<BigDecimal> jsonStateDescriptor = new ValueStateDescriptor<>("amount", BigDecimal.class);
                                jsonStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10L)).build());
                                jsonState = getRuntimeContext().getState(jsonStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                BigDecimal value = jsonState.value();
                                BigDecimal finalAmount = in.getBigDecimal("final_amount");
                                if (value != null) {
                                    in.put("final_amount", finalAmount.subtract(value));
                                }
                                in.remove("id");
                                in.remove("order_id");
                                in.remove("course_id");
                                in.remove("province_id");
                                in.remove("date_id");
                                in.remove("session_id");
                                in.remove("source_id");
                                in.remove("create_time");
                                in.remove("origin_amount");
                                in.remove("coupon_reduce_amount");
                                in.remove("out_trade_no");
                                in.remove("trade_body");
                                collector.collect(in);
                                jsonState.update(finalAmount);
                            }
                        }
                )
                .keyBy(o -> o.getString("user_id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            ValueState<String> userState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> userStateDescriptor = new ValueStateDescriptor<>("date", String.class);
                                userStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                                userState = getRuntimeContext().getState(userStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                in.remove("user_id");
                                in.put("ts", in.getLong("ts") * 1000);
                                in.put("uv", 0L);
                                in.put("vv", 1L);
                                String value = userState.value();
                                String date = DateFormatUtil.tsToDateTime(in.getLong("ts"));
                                if (!date.equals(value)) {
                                    in.put("uv", 1L);
                                }
                                collector.collect(in);
                                userState.update(date);
                            }
                        }
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                        .withTimestampAssigner((o, l) -> o.getLong("ts"))
                )
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(1L)))
                .reduce(new ReduceFunction<JSONObject>() {
                            @Override
                            public JSONObject reduce(JSONObject result, JSONObject in) throws Exception {
                                result.put("uv", result.getLong("uv") + in.getLong("uv"));
                                result.put("vv", result.getLong("vv") + in.getLong("vv"));
                                result.put("final_amount", result.getBigDecimal("final_amount").add(in.getBigDecimal("final_amount")));
                                return result;
                            }
                        },
                        new ProcessAllWindowFunction<JSONObject, JSONObject, TimeWindow>() {
                            @Override
                            public void process(ProcessAllWindowFunction<JSONObject, JSONObject, TimeWindow>.Context context, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
                                JSONObject next = iterable.iterator().next();
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                next.put("start_time", DateFormatUtil.tsToDateTime(start));
                                next.put("end_time", DateFormatUtil.tsToDateTime(end));
                                next.put("current_date", DateFormatUtil.tsToDate(start));
                                next.remove("ts");
                                collector.collect(next);
                            }
                        }
                );
        result.print();
        result.addSink(FlinkSinkUtil.getDorisSink(
                "dws_trade_total_window",
                "start_time",
                "end_time",
                "current_date",
                "final_amount",
                "uv",
                "vv"));
    }
}
