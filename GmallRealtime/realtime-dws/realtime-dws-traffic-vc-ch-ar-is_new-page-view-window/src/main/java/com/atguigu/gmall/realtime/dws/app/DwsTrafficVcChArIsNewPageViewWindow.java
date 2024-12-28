package com.atguigu.gmall.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.base.BaseApp;
import com.atguigu.gmall.realtime.common.bean.TrafficPageViewBean;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.function.BeanToJsonStrMapFunction;
import com.atguigu.gmall.realtime.common.util.DateFormatUtil;
import com.atguigu.gmall.realtime.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class DwsTrafficVcChArIsNewPageViewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficVcChArIsNewPageViewWindow().start(10022, 4, "dws_traffic_vc_ch_ar_is_new_page_view_window", Constant.TOPIC_DWD_TRAFFIC_PAGE);
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        kafkaStrDS.map(JSON::parseObject)
                // 按mid分组
                .keyBy(o -> o.getJSONObject("common").getString("mid"))
                // 将 JSONObject 处理为 TrafficPageViewBean
                .process(
                        new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
                            ValueState<String> lastViewState;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("lastViewState", String.class);
                                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                lastViewState = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject jsonObject, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context context, Collector<TrafficPageViewBean> collector) throws Exception {
                                JSONObject common = jsonObject.getJSONObject("common");
                                JSONObject page = jsonObject.getJSONObject("page");
                                Long ts = jsonObject.getLong("ts");
                                long uv = 0L;
                                String date = DateFormatUtil.tsToDate(ts);
                                String stateDate = lastViewState.value();
                                if (!date.equals(stateDate)) {
                                    uv = 1L;
                                    lastViewState.update(date);
                                }
                                collector.collect(
                                        new TrafficPageViewBean(
                                                "",
                                                "",
                                                date, // 当天日期
                                                common.getString("vc"), // app 版本号
                                                common.getString("ch"), // 渠道
                                                common.getString("ar"), // 地区
                                                common.getString("is_new"), // 新老访客状态标记
                                                uv, // 独立访客数
                                                page.getString("last_page_id") == null ? 1L : 0L, // 会话数
                                                1L, // 页面浏览数
                                                page.getLong("during_time"), // 累计访问时长
                                                ts // 时间戳
                                        )
                                );
                            }
                        }
                )
                // 生成水位线
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TrafficPageViewBean>forBoundedOutOfOrderness(Duration.ofMillis(0))
                                .withTimestampAssigner((trafficPageViewBean, l) -> trafficPageViewBean.getTs())
                )
                // 按 版本-渠道-地区-访客 分组
                .keyBy(o -> o.getVc() + o.getCh() + o.getAr() + o.getIsNew())
                // 设置窗口
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)))
                // 进行聚合
                .reduce(
                        new ReduceFunction<TrafficPageViewBean>() {
                            @Override
                            public TrafficPageViewBean reduce(TrafficPageViewBean result, TrafficPageViewBean in) {
                                result.setUvCt(result.getUvCt() + in.getUvCt());
                                result.setSvCt(result.getSvCt() + in.getSvCt());
                                result.setPvCt(result.getPvCt() + in.getPvCt());
                                result.setDurSum(result.getDurSum() + in.getDurSum());
                                return result;
                            }
                        },
                        new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, String, TimeWindow>.Context context, Iterable<TrafficPageViewBean> iterable, Collector<TrafficPageViewBean> collector) {
                                TrafficPageViewBean o = iterable.iterator().next();
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                o.setStt(start);
                                o.setEdt(end);
                                collector.collect(o);
                            }
                        }
                )
                // 将 TrafficPageViewBean 转换为JSONObject
                .map(new BeanToJsonStrMapFunction<>())
                .sinkTo(FlinkSinkUtil.getDorisSink("dws_traffic_vc_ch_ar_is_new_page_view_window"));
    }
}
