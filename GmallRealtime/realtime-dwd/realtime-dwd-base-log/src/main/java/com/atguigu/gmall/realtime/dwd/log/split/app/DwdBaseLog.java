package com.atguigu.gmall.realtime.dwd.log.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) throws Exception {
        new DwdBaseLog().start(10011, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        // 1. 对流中数据进行类型转换与ETL jsonStr -> jsonObj
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) {
                        try {
                            collector.collect(JSON.parseObject(s));
                        } catch (Exception e) {
                            context.output(dirtyTag, s);
                        }
                    }
                }
        );
        SideOutputDataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
        dirtyDS.sinkTo(FlinkSinkUtil.getKafkaSink("dirty_data"));

        // 2. 修复新老访客标记
        SingleOutputStreamOperator<JSONObject> fixedDS = jsonObjDS
                .keyBy(o -> o.getJSONObject("common").getString("mid"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            ValueState<String> lastVisitDateState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("lastVisitDataState", String.class);
                                lastVisitDateState = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                String isNew = jsonObject.getJSONObject("common").getString("is_new");
                                String value = lastVisitDateState.value();
                                long ts = Long.parseLong(jsonObject.getString("ts"));
                                if ("1".equals(isNew)) {
                                    // 如果标记是新用户
                                    String date = DateFormatUtil.tsToDate(ts);
                                    if (value == null || value.trim().isEmpty())
                                        // 如果状态为空，更新状态
                                        lastVisitDateState.update(date);
                                    else if (!value.equals(date))
                                        // 如果状态不为空，且状态与当前日期不同，修改标记为老用户
                                        jsonObject.getJSONObject("common").put("is_new", "0");
                                } else if ("0".equals(isNew) && (value == null || value.trim().isEmpty()))
                                    // 如果标记为老用户，且状态为空，更新状态为当前日期减去24小时
                                    lastVisitDateState.update(DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000));
                                collector.collect(jsonObject);
                            }
                        }
                );

        // 3. 日志分流
        OutputTag<String> errTag = new OutputTag<>("errTag", Types.STRING);
        OutputTag<String> startTag = new OutputTag<>("startTag", Types.STRING);
        OutputTag<String> displayTag = new OutputTag<>("displayTag", Types.STRING);
        OutputTag<String> actionTag = new OutputTag<>("actionTag", Types.STRING);
        SingleOutputStreamOperator<String> pageDS = fixedDS.process(
                new ProcessFunction<JSONObject, String>() {
                    @Override
                    public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) {
                        // 处理错误日志
                        if (jsonObject.getJSONObject("err") != null) {
                            context.output(errTag, jsonObject.toJSONString());
                            jsonObject.remove("err");
                        }
                        if (jsonObject.getJSONObject("start") != null) {
                            // 处理启动日志
                            context.output(startTag, jsonObject.toJSONString());
                        } else {
                            // 处理页面日志
                            JSONObject common = jsonObject.getJSONObject("common");
                            JSONObject page = jsonObject.getJSONObject("page");
                            Long ts = jsonObject.getLong("ts");
                            //  处理曝光日志
                            JSONArray displays = jsonObject.getJSONArray("displays");
                            if (displays != null && !displays.isEmpty()) {
                                for (int i = 0; i < displays.size(); ++i) {
                                    JSONObject display = displays.getJSONObject(i);
                                    JSONObject displayJsonObject = new JSONObject();
                                    displayJsonObject.put("common", common);
                                    displayJsonObject.put("page", page);
                                    displayJsonObject.put("display", display);
                                    displayJsonObject.put("ts", ts);
                                    context.output(displayTag, displayJsonObject.toJSONString());
                                }
                                jsonObject.remove("displays");
                            }
                            //  处理动作日志
                            JSONArray actions = jsonObject.getJSONArray("actions");
                            if (actions != null && !actions.isEmpty()) {
                                for (int i = 0; i < actions.size(); ++i) {
                                    JSONObject action = actions.getJSONObject(i);
                                    JSONObject actionJsonObject = new JSONObject();
                                    actionJsonObject.put("common", common);
                                    actionJsonObject.put("page", page);
                                    actionJsonObject.put("action", action);
                                    context.output(actionTag, actionJsonObject.toJSONString());
                                }
                                jsonObject.remove("actions");
                            }
                            collector.collect(jsonObject.toJSONString());
                        }
                    }
                }
        );
        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
    }
}