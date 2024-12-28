package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TradeSkuOrderBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.function.DimAsyncFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsTradeSkuOrderWindowSyncCache extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeSkuOrderWindowSyncCache().start(10029, 4, "dws_trade_sku_order_window", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<TradeSkuOrderBean> ds = kafkaStrDS
                .filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .keyBy(o -> o.getString("id"))
                .process(
                        // 状态 + 定时器 去重
                        //new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
                        //    ValueState<JSONObject> state;
                        //
                        //    @Override
                        //    public void open(Configuration parameters) throws Exception {
                        //        ValueStateDescriptor<JSONObject> jsonObjectValueStateDescriptor = new ValueStateDescriptor<>("state", JSONObject.class);
                        //        state = getRuntimeContext().getState(jsonObjectValueStateDescriptor);
                        //    }
                        //
                        //    @Override
                        //    public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {
                        //        if (state.value() == null) {
                        //            TimerService timerService = context.timerService();
                        //            timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 5000L);
                        //        }
                        //        state.update(jsonObject);
                        //    }
                        //
                        //    @Override
                        //    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.OnTimerContext ctx, Collector<TradeSkuOrderBean> out) throws Exception {
                        //        JSONObject stateValue = state.value();
                        //TradeSkuOrderBean tradeSkuOrderBean = TradeSkuOrderBean.builder()
                        //        .orderDetailId(stateValue.getString("id"))
                        //        .skuId(stateValue.getString("sku_id"))
                        //        .originalAmount(stateValue.getBigDecimal("split_original_amount"))
                        //        .couponReduceAmount(stateValue.getBigDecimal("split_coupon_amount"))
                        //        .activityReduceAmount(stateValue.getBigDecimal("split_activity_amount"))
                        //        .orderAmount(stateValue.getBigDecimal("split_total_amount"))
                        //        .ts(stateValue.getLong("ts") * 1000)
                        //        .build();
                        //        out.collect(tradeSkuOrderBean);
                        //        state.clear();
                        //    }
                        //}

                        // 状态 + 抵消 去重
                        new KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>() {
                            ValueState<JSONObject> state;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<JSONObject> jsonObjectValueStateDescriptor = new ValueStateDescriptor<>("state", JSONObject.class);
                                state = getRuntimeContext().getState(jsonObjectValueStateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TradeSkuOrderBean>.Context context, Collector<TradeSkuOrderBean> collector) throws Exception {
                                JSONObject stateValue = state.value();
                                if (stateValue != null) {
                                    collector.collect(TradeSkuOrderBean.builder()
                                            .orderDetailId(stateValue.getString("id"))
                                            .skuId(stateValue.getString("sku_id"))
                                            .originalAmount(stateValue.getBigDecimal("split_original_amount").negate())
                                            .couponReduceAmount(stateValue.getBigDecimal("split_coupon_amount").negate())
                                            .activityReduceAmount(stateValue.getBigDecimal("split_activity_amount").negate())
                                            .orderAmount(stateValue.getBigDecimal("split_total_amount").negate())
                                            .ts(stateValue.getLong("ts") * 1000)
                                            .curDate(DateFormatUtil.tsToDate(stateValue.getLong("ts") * 1000))
                                            .build());
                                }
                                collector.collect(TradeSkuOrderBean.builder()
                                        .orderDetailId(jsonObject.getString("id"))
                                        .skuId(jsonObject.getString("sku_id"))
                                        .originalAmount(jsonObject.getBigDecimal("split_original_amount"))
                                        .couponReduceAmount(jsonObject.getBigDecimal("split_coupon_amount"))
                                        .activityReduceAmount(jsonObject.getBigDecimal("split_activity_amount"))
                                        .orderAmount(jsonObject.getBigDecimal("split_total_amount"))
                                        .ts(jsonObject.getLong("ts") * 1000)
                                        .curDate(DateFormatUtil.tsToDate(jsonObject.getLong("ts") * 1000))
                                        .build());
                                state.update(jsonObject);
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<TradeSkuOrderBean>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                                .withTimestampAssigner((o, l) -> o.getTs())
                )
                .keyBy(TradeSkuOrderBean::getSkuId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        (ReduceFunction<TradeSkuOrderBean>) (result, in) -> {
                            result.setOriginalAmount(result.getOriginalAmount().add(in.getOriginalAmount()));
                            result.setCouponReduceAmount(result.getCouponReduceAmount().add(in.getCouponReduceAmount()));
                            result.setActivityReduceAmount(result.getActivityReduceAmount().add(in.getActivityReduceAmount()));
                            result.setOrderAmount(result.getOrderAmount().add(in.getOrderAmount()));
                            return result;
                        },
                        new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context, Iterable<TradeSkuOrderBean> iterable, Collector<TradeSkuOrderBean> collector) {
                                TradeSkuOrderBean next = iterable.iterator().next();
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                next.setStt(start);
                                next.setEdt(end);
                                collector.collect(next);
                            }
                        }
                );
        // 关联维度数据
        // - version 1 基本方式
        //.map(
        //        new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
        //            Connection hbaseConnection;
        //
        //            @Override
        //            public void open(Configuration parameters) {
        //                hbaseConnection = HBaseUtil.getHbaseConnection();
        //            }
        //
        //            @Override
        //            public void close() throws Exception {
        //                HBaseUtil.closeHBaseConnection(hbaseConnection);
        //            }
        //
        //            @Override
        //            public TradeSkuOrderBean map(TradeSkuOrderBean in) throws Exception {
        //                String skuId = in.getSkuId();
        //                // 关联 sku_info 维度表
        //                JSONObject skuInfo = HBaseUtil.getRow(hbaseConnection,Constant.HBASE_NAMESPACE,"dim_sku_info",skuId,JSONObject.class,false);
        //                in.setTrademarkId(skuInfo.getString("tm_id"));
        //                in.setSkuName(skuInfo.getString("sku_name"));
        //                in.setSpuId(skuInfo.getString("spu_id"));
        //                in.setCategory3Id(skuInfo.getString("category3_id"));
        //
        //                // 关联 spu_info 维度表
        //                JSONObject spuInfo = HBaseUtil.getRow(hbaseConnection,Constant.HBASE_NAMESPACE,"dim_spu_info",in.getSpuId(),JSONObject.class,false);
        //                in.setSpuName(spuInfo.getString("spu_name"));
        //
        //                // 关联 base_trademark 维度表
        //                JSONObject trademark = HBaseUtil.getRow(hbaseConnection,Constant.HBASE_NAMESPACE,"dim_base_trademark",in.getTrademarkId(),JSONObject.class,false);
        //                in.setTrademarkName(trademark.getString("tm_name"));
        //
        //                // 关联 base_category3 维度表
        //                JSONObject category3 = HBaseUtil.getRow(hbaseConnection,Constant.HBASE_NAMESPACE,"dim_base_category3",in.getCategory3Id(),JSONObject.class,false);
        //                in.setCategory3Name(category3.getString("name"));
        //                in.setCategory2Id(category3.getString("category2_id"));
        //
        //
        //                // 关联 base_category2 维度表
        //                JSONObject category2 = HBaseUtil.getRow(hbaseConnection,Constant.HBASE_NAMESPACE,"dim_base_category2",in.getCategory2Id(),JSONObject.class,false);
        //                in.setCategory2Name(category2.getString("name"));
        //                in.setCategory1Id(category2.getString("category1_id"));
        //
        //                // 关联 base_category1 维度表
        //                JSONObject category1 = HBaseUtil.getRow(hbaseConnection,Constant.HBASE_NAMESPACE,"dim_base_category1",in.getCategory1Id(),JSONObject.class,false);
        //                in.setCategory1Name(category1.getString("name"));
        //
        //                return in;
        //            }
        //        }
        //);
        // - version 2 旁路缓存
        //.map(new DimMapFunction<TradeSkuOrderBean>() {
        //    @Override
        //    public String getRowKey(TradeSkuOrderBean in) {
        //        return in.getSkuId();
        //    }
        //
        //    @Override
        //    public String getTableName(TradeSkuOrderBean in) {
        //        return "dim_sku_info";
        //    }
        //
        //    @Override
        //    public void addDim(TradeSkuOrderBean in, JSONObject dimJson) {
        //        in.setTrademarkId(dimJson.getString("tm_id"));
        //        in.setSkuName(dimJson.getString("sku_name"));
        //        in.setSpuId(dimJson.getString("spu_id"));
        //        in.setCategory3Id(dimJson.getString("category3_id"));
        //    }
        //});
        // - v3 旁路缓存 + 异步IO
        SingleOutputStreamOperator<TradeSkuOrderBean> withSkuInfoDs = AsyncDataStream.unorderedWait(
                ds,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean in) {
                        return in.getSkuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_sku_info";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean in, JSONObject dimJson) {
                        in.setTrademarkId(dimJson.getString("tm_id"));
                        in.setSkuName(dimJson.getString("sku_name"));
                        in.setSpuId(dimJson.getString("spu_id"));
                        in.setCategory3Id(dimJson.getString("category3_id"));
                    }
                },
                60, TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = AsyncDataStream.unorderedWait(
                withSkuInfoDs,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean in) {
                        return in.getSpuId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_spu_info";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean in, JSONObject dimJson) {
                        in.setSpuName(dimJson.getString("spu_name"));
                    }
                },
                60, TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> withTrademarkDS = AsyncDataStream.unorderedWait(
                withSpuInfoDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean in) {
                        return in.getTrademarkId();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_trademark";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean in, JSONObject dimJson) {
                        in.setTrademarkName(dimJson.getString("tm_name"));
                    }
                },
                60, TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory3DS = AsyncDataStream.unorderedWait(
                withTrademarkDS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean in) {
                        return in.getCategory3Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category3";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean in, JSONObject dimJson) {
                        in.setCategory3Name(dimJson.getString("name"));
                        in.setCategory2Id(dimJson.getString("category2_id"));
                    }
                },
                60, TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory2DS = AsyncDataStream.unorderedWait(
                withCategory3DS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean in) {
                        return in.getCategory2Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category2";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean in, JSONObject dimJson) {
                        in.setCategory2Name(dimJson.getString("name"));
                        in.setCategory1Id(dimJson.getString("category1_id"));
                    }
                },
                60, TimeUnit.SECONDS
        );
        SingleOutputStreamOperator<TradeSkuOrderBean> withCategory1DS = AsyncDataStream.unorderedWait(
                withCategory2DS,
                new DimAsyncFunction<TradeSkuOrderBean>() {
                    @Override
                    public String getRowKey(TradeSkuOrderBean in) {
                        return in.getCategory1Id();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_category1";
                    }

                    @Override
                    public void addDim(TradeSkuOrderBean in, JSONObject dimJson) {
                        in.setCategory1Name(dimJson.getString("name"));
                    }
                },
                60, TimeUnit.SECONDS
        );
        withCategory1DS.print();
        withCategory1DS.map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));
    }
}