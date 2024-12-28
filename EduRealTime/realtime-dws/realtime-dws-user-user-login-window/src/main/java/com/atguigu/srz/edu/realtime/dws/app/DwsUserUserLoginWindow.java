package com.atguigu.srz.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.bean.UserLoginBean;
import com.atguigu.edu.common.constant.Constant;
import com.atguigu.edu.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class DwsUserUserLoginWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsUserUserLoginWindow().start(
                10025,
                4,
                "dws_user_user_login_window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }
    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        //TODO 1.转格式
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaStrDS.map(JSON::parseObject);

        //TODO 2.过滤出登陆数据
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDs.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) throws Exception {
                        String uid = jsonObj.getJSONObject("common").getString("uid");
                        String lastPageId = jsonObj.getJSONObject("page").getString("last_page_id");
                        return StringUtils.isNotEmpty(uid) && ("login".equals(lastPageId) || StringUtils.isEmpty(lastPageId));
                    }
                }
        );
        //filterDS.print("filterDS");\
        
        //TODO 3.设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkDs = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps().withTimestampAssigner(
                        new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                return jsonObj.getLong("ts");
                            }
                        }
                )
        );

        //TODO 4.按照uid进行分组
        KeyedStream<JSONObject, String> keyedDs = withWatermarkDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("uid"));
        
        //TODO 5.统计
        SingleOutputStreamOperator<UserLoginBean> beanDs = keyedDs.process(new KeyedProcessFunction<String, JSONObject, UserLoginBean>() {
            ValueState<String> lastLoginDateState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("lastLoginDate", String.class);
                lastLoginDateState = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, UserLoginBean>.Context ctx, Collector<UserLoginBean> out) throws Exception {
                //从状态中获取上次登录日期
                String lastLoginDate = lastLoginDateState.value();

                //获取当前登录日期
                Long ts = jsonObj.getLong("ts");
                String curLoginDate = DateFormatUtil.tsToDate(ts);

                //声明首次登录标记
                Long nuCt = 0L;

                //活跃用户标记
                Long actCt = 0L;

                //回流用户标记
                Long backCt = 0L;

                if (StringUtils.isEmpty(lastLoginDate)) {
                    nuCt = 1L;
                    lastLoginDateState.update(curLoginDate);
                } else {
                    Long days = (ts - DateFormatUtil.dateToTs(lastLoginDate)) / 1000 / 60 / 60 / 24;
                    if (days <= 7) {
                        actCt = 1L;
                    } else {
                        backCt = 1L;
                    }
                }
                if (nuCt != 0L || actCt!=0L || actCt!=0L){
                UserLoginBean userLoginBean = new UserLoginBean(
                        "",
                        "",
                        "",
                        nuCt,
                        backCt,
                        actCt,
                        ts
                );
                out.collect(userLoginBean);
                }
            }
        });

        //TODO 6.开窗
        AllWindowedStream<UserLoginBean, TimeWindow> windowDs
                = beanDs.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));

        //TODO 7.聚合
        SingleOutputStreamOperator<UserLoginBean> reduceDs = windowDs.reduce(
                new ReduceFunction<UserLoginBean>() {
                    @Override
                    public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                        value1.setNuCt(value1.getNuCt() + value2.getNuCt());
                        value1.setActCt(value1.getActCt() + value2.getActCt());
                        value1.setBackCt(value1.getBackCt() + value2.getBackCt());
                        return value1;
                    }
                },
                new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {
                        UserLoginBean bean = values.iterator().next();
                        bean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                        bean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                        bean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                        out.collect(bean);
                    }
                }
        );

        //TODO 8.将结果写入Doris
        reduceDs.print("reduceDs");
        reduceDs
                .map(new BeanToJsonStrMapFunction<>())
                .map(JSON::parseObject)
                .addSink(FlinkSinkUtil.getDorisSink(
                        "dws_user_user_login_window",
                        "stt",
                        "edt",
                        "cur_date",
                        "nu_ct",
                        "back_ct",
                        "act_ct"
                ));
    }
}
