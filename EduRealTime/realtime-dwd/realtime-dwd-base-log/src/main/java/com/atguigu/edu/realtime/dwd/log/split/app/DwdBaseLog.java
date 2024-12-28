package com.atguigu.edu.realtime.dwd.log.split.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.constant.Constant;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class DwdBaseLog extends BaseApp {
    public static void main(String[] args) {
        new DwdBaseLog().start(10021, 4, "dwd_base_log", Constant.TOPIC_LOG);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        //TODO 1.对流中的数据进行转换，jsonStr->jsonObj，并对脏数据进行过滤
        //脏数据放侧输出流
        OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {
        };
        //标准数据向下游传递
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        } catch (Exception e) {
                            ctx.output(dirtyTag, jsonStr);
                        }
                    }
                }
        );
        //jsonObjDs.print();
        //主流接收侧输出流数据
        SideOutputDataStream<String> dirtyDs = jsonObjDs.getSideOutput(dirtyTag);
        //脏数据写入Kafka主题
        KafkaSink<String> kafkaSink = FlinkSinkUtil.getKafkaSink("dirty_data");
        dirtyDs.sinkTo(kafkaSink);


        //TODO 2.修复新老访客标记
        //2.1 按照mid进行分组
        KeyedStream<JSONObject, String> keyedDs = jsonObjDs.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );
        //2.2 使用Flink的状态编程修复新老访客标记
        SingleOutputStreamOperator<JSONObject> fixedDs = keyedDs.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<String>("lastVisitDataState", String.class);
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                        //获取isnew字段
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");

                        //从状态中获取信息
                        String lastVisitState = lastVisitDateState.value();

                        //获取当前访问日期
                        Long ts = jsonObj.getLong("ts");
                        String curDate = DateFormatUtil.tsToDate(ts);

                        //处理逻辑
                        if ("1".equals(isNew)) {
                            if (StringUtils.isEmpty(lastVisitState)) {
                                lastVisitDateState.update(curDate);
                            } else {
                                if (!lastVisitState.equals(curDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (StringUtils.isEmpty(lastVisitState)) {
                                String yesterday = DateFormatUtil.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesterday);
                            }
                        }
                        out.collect(jsonObj);
                    }
                }
        );
        //fixedDs.print("fixedDs");

        //TODO 3.日志分流
        //3.1 定义侧输出流标签
        OutputTag<String> errTag = new OutputTag<String>("errTag") {
        };
        OutputTag<String> startTag = new OutputTag<String>("startTag") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("displayTag") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("actionTag") {
        };
        OutputTag<String> appVideoTag = new OutputTag<String>("appVideoTag") {
        };

        SingleOutputStreamOperator<String> pageDS = fixedDs.process(
                new ProcessFunction<JSONObject, String>() {

                    @Override
                    public void processElement(JSONObject jsonObj, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {

                        //错误日志输入到errTag侧输出流
                        JSONObject errJsonObj = jsonObj.getJSONObject("err");
                        if (errJsonObj != null) {
                            ctx.output(errTag, jsonObj.toJSONString());
                            jsonObj.remove("err");
                        }


                        //启动日志的处理
                        JSONObject startJsonObj = jsonObj.getJSONObject("start");
                        JSONObject appVideo = jsonObj.getJSONObject("appVideo");
                        if (startJsonObj != null) {
                            ctx.output(startTag, jsonObj.toJSONString());
                        } else if (appVideo != null) {
                            //播放日志的处理
                            ctx.output(appVideoTag, jsonObj.toJSONString());
                            jsonObj.remove("appVideo");
                        } else {
                            //页面日志的处理
                            JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                            JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                            Long ts = jsonObj.getLong("ts");

                            //曝光日志的处理
                            JSONArray displayArr = jsonObj.getJSONArray("displays");
                            if (displayArr != null && displayArr.size() > 0) {
                                for (int i = 0; i < displayArr.size(); i++) {
                                    //获取数组中每一条曝光数据
                                    JSONObject displayJsonObj = displayArr.getJSONObject(i);
                                    //定义一个json对象，把每一条曝光数据的common和page信息补充进去
                                    JSONObject newDisplayJsonObj = new JSONObject();
                                    newDisplayJsonObj.put("common", commonJsonObj);
                                    newDisplayJsonObj.put("page", pageJsonObj);
                                    newDisplayJsonObj.put("display", displayJsonObj);
                                    newDisplayJsonObj.put("ts", ts);
                                    //曝光数据写入侧输出流
                                    ctx.output(displayTag, newDisplayJsonObj.toJSONString());
                                }
                                jsonObj.remove("displays");
                            }

                            //动作日志的处理
                            JSONArray actionArr = jsonObj.getJSONArray("actions");
                            if (actionArr != null && actionArr.size() > 0) {
                                for (int i = 0; i < actionArr.size(); i++) {
                                    JSONObject actionJsonObj = actionArr.getJSONObject(i);
                                    JSONObject newActionJsonObj = new JSONObject();
                                    newActionJsonObj.put("common", commonJsonObj);
                                    newActionJsonObj.put("page", pageJsonObj);
                                    newActionJsonObj.put("action", actionJsonObj);
                                    //将动作日志放到动作侧输出流
                                    ctx.output(actionTag, newActionJsonObj.toJSONString());
                                }
                                jsonObj.remove("actions");
                            }
                            out.collect(jsonObj.toJSONString());


                        }
                    }

                }
        );

        SideOutputDataStream<String> errDS = pageDS.getSideOutput(errTag);
        SideOutputDataStream<String> startDS = pageDS.getSideOutput(startTag);
        SideOutputDataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        SideOutputDataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        SideOutputDataStream<String> appVideoDs = pageDS.getSideOutput(appVideoTag);

        errDS.print("err:");
        startDS.print("start:");
        displayDS.print("display:");
        actionDS.print("action:");
        appVideoDs.print("appVideo");
        pageDS.print("page:");

        //TODO 4.将不同流的数据写到kafka的不同主题
        pageDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_PAGE));
        errDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ERR));
        startDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_START));
        displayDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_DISPLAY));
        actionDS.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_ACTION));
        appVideoDs.sinkTo(FlinkSinkUtil.getKafkaSink(Constant.TOPIC_DWD_TRAFFIC_APPVIDEO));
    }
}