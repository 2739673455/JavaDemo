package com.atguigu.edu.realtime.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.function.DimAsyncFunction;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10046, 4, "dws_trade_province_order_window", "dwd_trade_order_detail");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> ds = kafkaStrDS.filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .keyBy(o -> o.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<BigDecimal> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<BigDecimal> descriptor = new ValueStateDescriptor<BigDecimal>("amount", BigDecimal.class);
                        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        state = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        BigDecimal value = state.value();
                        value = value == null ? BigDecimal.ZERO : value;
                        state.update(in.getBigDecimal("final_amount"));
                        in.put("final_amount", in.getBigDecimal("final_amount").subtract(value));
                        in.remove("id");
                        in.remove("order_id");
                        in.remove("course_id");
                        in.remove("date_id");
                        in.remove("session_id");
                        in.remove("source_id");
                        in.remove("create_time");
                        in.remove("origin_amount");
                        in.remove("coupon_reduce_amount");
                        in.remove("out_trade_no");
                        in.remove("trade_body");
                        in.put("ts", in.getLong("ts") * 1000);
                        collector.collect(in);
                    }
                })
                .keyBy(o -> o.getString("province_id") + ":" + o.getString("id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> descriptor = new ValueStateDescriptor<String>("date", String.class);
                        descriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        state = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        in.put("order_count", 1L);
                        in.put("uu_order_count", 0L);
                        String value = state.value();
                        String date = DateFormatUtil.tsToDateTime(in.getLong("ts"));
                        if (!date.equals(value)) {
                            in.put("uu_order_count", 1L);
                            state.update(date);
                        }
                        collector.collect(in);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                        .withTimestampAssigner((o, l) -> o.getLong("ts")))
                .keyBy(o -> o.getString("province_id"))
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<JSONObject>() {
                            @Override
                            public JSONObject reduce(JSONObject result, JSONObject in) throws Exception {
                                result.put("final_amount", result.getBigDecimal("final_amount").add(in.getBigDecimal("final_amount")));
                                result.put("order_count", result.getLong("order_count") + in.getLong("order_count"));
                                result.put("uu_order_count", result.getLong("uu_order_count") + in.getLong("uu_order_count"));
                                return result;
                            }
                        },
                        new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
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
        SingleOutputStreamOperator<JSONObject> withProvinceName = AsyncDataStream.unorderedWait(ds,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public String getRowKey(JSONObject in) {
                        return in.getString("province_id");
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public void addDim(JSONObject in, JSONObject dimJson) {
                        in.put("province_name", dimJson.getString("name"));
                    }
                },
                60, TimeUnit.SECONDS);

        withProvinceName.print();
        withProvinceName.addSink(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window",
                "start_time",
                "end_time",
                "current_date",
                "province_id",
                "province_name",
                "final_amount",
                "order_count",
                "uu_order_count"));
    }
}
