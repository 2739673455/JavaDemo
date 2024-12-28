package com.atguigu.com.edu.realtime.dws.trade.cart.app.window.app;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.bean.TradeCartAddWindowBean;
import com.atguigu.edu.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
* 加购各窗口统计
* */
public class DwsTradeCartAddWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTradeCartAddWindow().start(10042,4,"dws_trade_cart_add_window", "dwd_trade_cart_add");
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        //TODO 对流中数据进行类型转换
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);
        //{"course_id":255,"create_time":"2024-11-15 14:56:38","user_id":4801,"course_name":"Vue技术全家桶","id":22507,"ts":1731653798000}
        // jsonObjDS.print();

        //TODO 指定Watermark的生成策略以及提取事件时间字段
        SingleOutputStreamOperator<JSONObject> withWaterMarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );
         //withWaterMarkDS.print();

        //TODO 按照用户ID进行分组
        KeyedStream<JSONObject, String> keyedDS = withWaterMarkDS.keyBy(jsonObj->jsonObj.getString("user_id"));

        //TODO 使用Flink状态编程 判断是否为加购独立用户
        SingleOutputStreamOperator<JSONObject> cartDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //获取当前加购日期
                        Long ts = value.getLong("ts");
                        String curCarDate = DateFormatUtil.tsToDate(ts);
                        //从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCarDate)) {
                            out.collect(value);
                            lastCartDateState.update(curCarDate);
                        }
                    }
                }
        );
        //cartDS.print();

        //TODO 开窗
        AllWindowedStream<JSONObject, TimeWindow> windowDS = cartDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        //TODO 聚合
        SingleOutputStreamOperator<TradeCartAddWindowBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, TradeCartAddWindowBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<TradeCartAddWindowBean> out) throws Exception {
                        Long cartUuCt = values.iterator().next();
                        out.collect(new TradeCartAddWindowBean(
                                DateFormatUtil.tsToDateTime(window.getStart()),
                                DateFormatUtil.tsToDateTime(window.getEnd()),
                                DateFormatUtil.tsToDate(window.getStart()),
                                cartUuCt
                        ));
                    }
                }

        );
        aggregateDS.print();
        aggregateDS.map(new BeanToJsonStrMapFunction<>()).map(JSON::parseObject)
                .addSink((FlinkSinkUtil.getDorisSink("dws_trade_province_order_window",
                "stt",
                "edt",
                "cur_date",
                "cart_add_uu_ct")));
    }

}
