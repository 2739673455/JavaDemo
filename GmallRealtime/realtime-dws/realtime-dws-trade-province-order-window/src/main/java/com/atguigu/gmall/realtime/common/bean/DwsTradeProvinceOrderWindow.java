package com.atguigu.gmall.realtime.common.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeProvinceOrderWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeProvinceOrderWindow().start(10020, 4, "dws_trade_province_order_window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<TradeProvinceOrderBean> ds = kafkaStrDS.filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .keyBy(o -> o.getString("id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, TradeProvinceOrderBean>() {
                            ValueState<TradeProvinceOrderBean> state;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<TradeProvinceOrderBean> valueStateDescriptor = new ValueStateDescriptor<>("state", TradeProvinceOrderBean.class);
                                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                state = getRuntimeContext().getState(valueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeProvinceOrderBean>.Context context, Collector<TradeProvinceOrderBean> collector) throws Exception {
                                TradeProvinceOrderBean value = state.value();
                                TradeProvinceOrderBean tradeProvinceOrderBean = TradeProvinceOrderBean.builder()
                                        .orderId(jsonObject.getString("order_id"))
                                        .provinceId(jsonObject.getString("province_id"))
                                        .orderAmount(jsonObject.getBigDecimal("split_total_amount"))
                                        .orderCount(1L)
                                        .ts(jsonObject.getLong("ts") * 1000)
                                        .build();
                                state.update(tradeProvinceOrderBean);
                                if (value != null) {
                                    tradeProvinceOrderBean.setOrderAmount(tradeProvinceOrderBean.getOrderAmount().subtract(value.getOrderAmount()));
                                }
                                collector.collect(tradeProvinceOrderBean);
                            }
                        }
                )
                .keyBy(TradeProvinceOrderBean::getOrderId)
                .reduce(
                        new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean result, TradeProvinceOrderBean in) throws Exception {
                                result.setOrderAmount(result.getOrderAmount().add(in.getOrderAmount()));
                                result.setOrderCount(1L);
                                return result;
                            }
                        }
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<TradeProvinceOrderBean>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                        .withTimestampAssigner((o, l) -> o.getTs()))
                .keyBy(TradeProvinceOrderBean::getProvinceId)
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                .reduce(
                        new ReduceFunction<TradeProvinceOrderBean>() {
                            @Override
                            public TradeProvinceOrderBean reduce(TradeProvinceOrderBean result, TradeProvinceOrderBean in) throws Exception {
                                result.setOrderAmount(result.getOrderAmount().add(in.getOrderAmount()));
                                result.setOrderCount(result.getOrderCount() + in.getOrderCount());
                                return result;
                            }
                        },
                        new ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>.Context context, Iterable<TradeProvinceOrderBean> iterable, Collector<TradeProvinceOrderBean> collector) throws Exception {
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                String date = DateFormatUtil.tsToDate(context.window().getStart());
                                TradeProvinceOrderBean next = iterable.iterator().next();
                                next.setStt(start);
                                next.setEdt(end);
                                next.setCurDate(date);
                                collector.collect(next);
                            }
                        }
                );
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceNameDS = AsyncDataStream.unorderedWait(ds,
                new DimAsyncFunction<TradeProvinceOrderBean>() {
                    @Override
                    public String getRowKey(TradeProvinceOrderBean in) {
                        return in.getProvinceId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_province";
                    }

                    @Override
                    public void addDim(TradeProvinceOrderBean in, JSONObject dimJson) {
                        in.setProvinceName(dimJson.getString("name"));
                    }
                },
                60, TimeUnit.SECONDS);
        withProvinceNameDS.print();
        withProvinceNameDS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));
    }
}
