package com.atguigu.edu.realtime.dws.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.bean.TrafficPageViewBean;
import com.atguigu.edu.common.constant.Constant;
import com.atguigu.edu.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.common.function.DimAsyncFunction;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class DwsTrafficScIsNewPageViesWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTrafficScIsNewPageViesWindow().start(
                10024,
                4,
                "dws-traffic-sc-is_new-page-view-window",
                Constant.TOPIC_DWD_TRAFFIC_PAGE
        );
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {

        //TODO 1.对流中数据进行类型转换    jsonStr->jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDs = kafkaStrDS.map(JSON::parseObject);


        //TODO 2.按照设备的id进行分组
        KeyedStream<JSONObject, String> keyedDS = jsonObjDs.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 3.对分组后的数据进行处理   jsonObj->统计的实体类对象  相当于WordCount封装二元组   使用Flink的状态编程，判断是否为当日独立访客
        SingleOutputStreamOperator<TrafficPageViewBean> beanDs = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>() {
                    //声明状态
                    ValueState<String> lastVisitDateState;

                    //初始化状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> statePropertiesDesc
                                = new ValueStateDescriptor<String>("lastVisitDateState", String.class);
                        statePropertiesDesc.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastVisitDateState = getRuntimeContext().getState(statePropertiesDesc);

                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, TrafficPageViewBean>.Context ctx, Collector<TrafficPageViewBean> out) throws Exception {
                        //获取common和page信息
                        JSONObject commonJsonObj = jsonObj.getJSONObject("common");
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");

                        //从状态中获取上次访问日期 和 当前的访问时间
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObj.getLong("ts");
                        String curDate = DateFormatUtil.tsToDate(ts);

                        //定义独立访客数
                        Long uvct = 0L;

                        //判断是否为独立访客
                        if (StringUtils.isEmpty(lastVisitDate) || !curDate.equals(lastVisitDate)) {
                            uvct = 1L;
                            lastVisitDateState.update(curDate);

                        }

                        //定义会话数并统计，上次访问页面为空的为新开的会话。
                        Long svct = StringUtils.isEmpty(pageJsonObj.getString("last_page_id")) ? 1L : 0L;

                        //数据封装进Bean
                        TrafficPageViewBean viewBean = new TrafficPageViewBean(
                                "",
                                "",
                                "",
                                commonJsonObj.getString("sc"),
                                "",
                                commonJsonObj.getString("sid"),
                                commonJsonObj.getString("is_new"),
                                uvct,
                                svct,
                                1L,
                                pageJsonObj.getLong("during_time"),
                                ts
                        );
                        out.collect(viewBean);
                    }
                }
        );

        //beanDs.print("beanDs");
        //TODO 4.指定Watermark以及提取事件时间字段

        SingleOutputStreamOperator<TrafficPageViewBean> withWatermarkDs = beanDs.assignTimestampsAndWatermarks(
                WatermarkStrategy.<TrafficPageViewBean>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<TrafficPageViewBean>() {
                            @Override
                            public long extractTimestamp(TrafficPageViewBean bean, long recordTimestamp) {
                                return bean.getTs();
                            }
                        })
        );

        //TODO 5.按照统计的维度进行分组

        KeyedStream<TrafficPageViewBean, Tuple3<String, String, String>> dimKeyedDs = withWatermarkDs.keyBy(
                new KeySelector<TrafficPageViewBean, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> getKey(TrafficPageViewBean bean) throws Exception {
                        return Tuple3.of(
                                bean.getSc(),
                                bean.getSid(),
                                bean.getIsNew()
                        );
                    }
                }
        );
//        dimKeyedDs.print("dimKeyedDs");
        //TODO 6.开窗
        WindowedStream<TrafficPageViewBean, Tuple3<String, String, String>, TimeWindow> windowDS = dimKeyedDs.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5)));

        //TODO 7.聚合
        SingleOutputStreamOperator<TrafficPageViewBean> reduceDs = windowDS.reduce(
                new ReduceFunction<TrafficPageViewBean>() {
                    @Override
                    public TrafficPageViewBean reduce(TrafficPageViewBean value1, TrafficPageViewBean value2) throws Exception {
                        value1.setPvCt(value1.getPvCt() + value2.getPvCt());
                        value1.setUvCt(value1.getUvCt() + value2.getUvCt());
                        value1.setSvCt(value1.getSvCt() + value2.getSvCt());
//                        System.out.println("进行了一波聚合操作");
                        return value1;
                    }
                },
                new ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple3<String, String, String>, TimeWindow>() {
                    @Override
                    public void process(Tuple3<String, String, String> stringStringStringTuple3, ProcessWindowFunction<TrafficPageViewBean, TrafficPageViewBean, Tuple3<String, String, String>, TimeWindow>.Context context, Iterable<TrafficPageViewBean> elements, Collector<TrafficPageViewBean> out) throws Exception {
                        TimeWindow window = context.window();
                        TrafficPageViewBean viewBean = elements.iterator().next();

                        viewBean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                        //System.out.println(DateFormatUtil.tsToDateTime(window.getStart()));
                        viewBean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                        //System.out.println(DateFormatUtil.tsToDateTime(window.getEnd()));
                        viewBean.setCur_date(DateFormatUtil.tsToDate(window.getStart()));

                        System.out.println("进行了窗口操作");
                        out.collect(viewBean);
                    }
                }
        );
        reduceDs.print("reduceDs");
        //TODO 8.关联
        SingleOutputStreamOperator<TrafficPageViewBean> withSourceSiteNameDs = AsyncDataStream.unorderedWait(
                reduceDs,
                new DimAsyncFunction<TrafficPageViewBean>() {
                    @Override
                    public String getRowKey(TrafficPageViewBean bean) {
                        return bean.getSc();
                    }

                    @Override
                    public String getTableName() {
                        return "dim_base_source";
                    }

                    @Override
                    public void addDim(TrafficPageViewBean trafficBean, JSONObject dimJson) {
                        trafficBean.setScName(dimJson.getString("source_site"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );
        //TODO 9.将聚合的结果写到Doris
        withSourceSiteNameDs.print("withSourceSiteNameDs");
        withSourceSiteNameDs
                .map(new BeanToJsonStrMapFunction<>())
                .map(JSON::parseObject)
                .addSink(FlinkSinkUtil.getDorisSink(
                        "dws_traffic_sc_is_new_page_view_window",
                        "stt",
                        "edt",
                        "cur_date",
                        "sc",
                        "sc_name",
                        "sid",
                        "is_new",
                        "uv_ct",
                        "sv_ct",
                        "pv_ct",
                        "dur_sum"
                ));

    }
}




