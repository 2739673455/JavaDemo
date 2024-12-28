package com.atguigu.edu.realtime.dws.userfunnel.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import com.atguigu.edu.common.util.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsUserFunnelWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserFunnelWindow().start(10116, 4, "dws-user-funnel-window", "dwd_traffic_page");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 首页、商品详情页 浏览计数
        SingleOutputStreamOperator<JSONObject> pageCountDS = kafkaStrDS.map(in -> {
                    JSONObject inO = JSON.parseObject(in);
                    String uid = inO.getJSONObject("common").getString("uid");
                    String page_id = inO.getJSONObject("page").getString("page_id");
                    Long ts = inO.getLong("ts");
                    JSONObject out = new JSONObject();
                    out.put("uid", uid);
                    out.put("page_id", page_id);
                    out.put("ts", ts);
                    return out;
                })
                .keyBy(o -> o.getString("uid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            ValueState<String> homeState;
                            ValueState<String> courseDetailState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("state", String.class);
                                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                homeState = getRuntimeContext().getState(stateDescriptor);
                                courseDetailState = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                in.put("home_count", 0L);
                                in.put("course_detail_count", 0L);
                                in.put("cart_add_count", 0L);
                                in.put("order_count", 0L);
                                in.put("pay_success_count", 0L);
                                String homeValue = homeState.value();
                                String courseDetailValue = courseDetailState.value();
                                String date = DateFormatUtil.tsToDateTime(in.getLong("ts"));
                                String pageId = in.getString("page_id");
                                in.remove("uid");
                                in.remove("page_id");
                                if ("home".equals(pageId) && !date.equals(homeValue)) {
                                    in.put("home_count", 1L);
                                    homeState.update(date);
                                    collector.collect(in);
                                }
                                if ("course_detail".equals(pageId) && !date.equals(courseDetailValue)) {
                                    in.put("course_detail_count", 1L);
                                    courseDetailState.update(date);
                                    collector.collect(in);
                                }
                            }
                        }
                );
        // 加购计数
        KafkaSource<String> cartAddSource = FlinkSourceUtil.getKafkaSource("dwd_trade_cart_add", "dws-user-funnel-window");
        SingleOutputStreamOperator<JSONObject> cartAddCountDS = env.fromSource(cartAddSource, WatermarkStrategy.noWatermarks(), "cartAdd")
                .filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .keyBy(o -> o.getString("user_id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            ValueState<String> cartAddState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("state", String.class);
                                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                cartAddState = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                in.put("home_count", 0L);
                                in.put("course_detail_count", 0L);
                                in.put("cart_add_count", 0L);
                                in.put("order_count", 0L);
                                in.put("pay_success_count", 0L);
                                String cartAddValue = cartAddState.value();
                                String date = DateFormatUtil.tsToDateTime(in.getLong("ts"));
                                if (!date.equals(cartAddValue)) {
                                    in.put("cart_add_count", 1L);
                                    cartAddState.update(date);
                                }
                                in.remove("course_id");
                                in.remove("create_time");
                                in.remove("user_id");
                                in.remove("course_name");
                                in.remove("id");
                                collector.collect(in);
                            }
                        }
                );
        // 下单计数
        KafkaSource<String> orderSource = FlinkSourceUtil.getKafkaSource("dwd_trade_order_detail", "dws-user-funnel-window");
        SingleOutputStreamOperator<JSONObject> orderCountDS = env.fromSource(orderSource, WatermarkStrategy.noWatermarks(), "order")
                .filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .keyBy(o -> o.getString("user_id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            ValueState<String> orderState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("state", String.class);
                                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                orderState = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                in.put("home_count", 0L);
                                in.put("course_detail_count", 0L);
                                in.put("cart_add_count", 0L);
                                in.put("order_count", 0L);
                                in.put("pay_success_count", 0L);
                                in.put("ts", in.getLong("ts") * 1000);
                                String orderValue = orderState.value();
                                String date = DateFormatUtil.tsToDateTime(in.getLong("ts"));
                                if (!date.equals(orderValue)) {
                                    in.put("order_count", 1L);
                                    orderState.update(date);
                                }
                                in.remove("id");
                                in.remove("order_id");
                                in.remove("user_id");
                                in.remove("course_id");
                                in.remove("province_id");
                                in.remove("date_id");
                                in.remove("session_id");
                                in.remove("source_id");
                                in.remove("create_time");
                                in.remove("origin_amount");
                                in.remove("coupon_reduce_amount");
                                in.remove("final_amount");
                                in.remove("out_trade_no");
                                in.remove("trade_body");
                                collector.collect(in);
                            }
                        }
                );
        // 支付成功计数
        KafkaSource<String> paySuccessSource = FlinkSourceUtil.getKafkaSource("dwd_trade_order_payment_success", "dws-user-funnel-window");
        SingleOutputStreamOperator<JSONObject> paySuccessDS = env.fromSource(paySuccessSource, WatermarkStrategy.noWatermarks(), "paySuccess")
                .filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .keyBy(o -> o.getString("user_id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            ValueState<String> paySuccessState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("state", String.class);
                                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                paySuccessState = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                in.put("home_count", 0L);
                                in.put("course_detail_count", 0L);
                                in.put("cart_add_count", 0L);
                                in.put("order_count", 0L);
                                in.put("pay_success_count", 0L);
                                in.put("ts", in.getLong("ts") * 1000);
                                String paySuccessValue = paySuccessState.value();
                                String date = DateFormatUtil.tsToDateTime(in.getLong("ts"));
                                if (!date.equals(paySuccessValue)) {
                                    in.put("pay_success_count", 1L);
                                    paySuccessState.update(date);
                                }
                                in.remove("id");
                                in.remove("user_id");
                                in.remove("total_amount");
                                in.remove("create_time");
                                collector.collect(in);
                            }
                        }
                );
        DataStream<JSONObject> unionDS = pageCountDS.union(cartAddCountDS, orderCountDS, paySuccessDS);
        SingleOutputStreamOperator<JSONObject> result = unionDS.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                        .withTimestampAssigner((o, l) -> o.getLong("ts"))
                )
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<JSONObject>() {
                            @Override
                            public JSONObject reduce(JSONObject result, JSONObject in) throws Exception {
                                result.put("home_count", result.getLong("home_count") + in.getLong("home_count"));
                                result.put("course_detail_count", result.getLong("course_detail_count") + in.getLong("course_detail_count"));
                                result.put("cart_add_count", result.getLong("cart_add_count") + in.getLong("cart_add_count"));
                                result.put("order_count", result.getLong("order_count") + in.getLong("order_count"));
                                result.put("pay_success_count", result.getLong("pay_success_count") + in.getLong("pay_success_count"));
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
        result.addSink(FlinkSinkUtil.getDorisSink("dws_user_funnel_window",
                "start_time",
                "end_time",
                "current_date",
                "home_count",
                "course_detail_count",
                "cart_add_count",
                "order_count",
                "pay_success_count"
        ));
    }
}