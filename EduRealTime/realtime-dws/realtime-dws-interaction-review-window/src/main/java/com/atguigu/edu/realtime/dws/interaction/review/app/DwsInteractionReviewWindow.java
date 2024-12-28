package com.atguigu.edu.realtime.dws.interaction.review.app;

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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsInteractionReviewWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsInteractionReviewWindow().start(10114, 1, "dws_interaction_review_window", "dwd_interaction_review");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> ds = kafkaStrDS.filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .keyBy(o -> o.getString("course_id") + ":" + o.getString("user_id"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    ValueState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("date", String.class);
                        stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1L)).build());
                        state = getRuntimeContext().getState(stateDescriptor);
                    }


                    @Override
                    public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        in.remove("id");
                        in.remove("create_time");
                        in.remove("user_id");
                        in.put("uv", 0L);
                        in.put("star5", 0L);
                        in.put("review_stars", 0L);
                        String value = state.value();
                        String date = DateFormatUtil.tsToDate(in.getLong("ts"));
                        if (!date.equals(value)) {
                            in.put("uv", 1L);
                            if (5L == in.getLong("review_stars")) {
                                in.put("star5", 1L);
                            }
                            collector.collect(in);
                        }
                        state.update(value);
                    }
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                        .withTimestampAssigner((o, l) -> o.getLong("ts")))
                .keyBy(o -> o.getString("course_id"))
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<JSONObject>() {
                            @Override
                            public JSONObject reduce(JSONObject result, JSONObject in) throws Exception {
                                result.put("uv", result.getLong("uv") + in.getLong("uv"));
                                result.put("star5", result.getLong("star5") + in.getLong("star5"));
                                result.put("review_stars", result.getLong("review_stars") + in.getLong("review_stars"));
                                return result;
                            }
                        },
                        new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                            @Override
                            public void process(String string, ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
                                JSONObject next = iterable.iterator().next();
                                long start = context.window().getStart();
                                long end = context.window().getEnd();
                                next.put("start_time", DateFormatUtil.tsToDateTime(start));
                                next.put("end_time", DateFormatUtil.tsToDateTime(end));
                                next.put("current_date", DateFormatUtil.tsToDate(start));
                                next.remove("ts");
                                collector.collect(next);
                            }
                        });
        SingleOutputStreamOperator<JSONObject> withCourseName = AsyncDataStream.unorderedWait(ds,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public String getRowKey(JSONObject in) {
                        return in.getString("course_id");
                    }

                    @Override
                    public String getTableName() {
                        return "dim_course_info";
                    }

                    @Override
                    public void addDim(JSONObject in, JSONObject dimJson) {
                        in.put("course_name", dimJson.getString("course_name"));
                    }
                },
                60, TimeUnit.SECONDS);
        withCourseName.print();
        withCourseName.addSink(FlinkSinkUtil.getDorisSink("dws_interaction_review_window",
                "start_time",
                "end_time",
                "current_date",
                "course_id",
                "course_name",
                "uv",
                "review_stars",
                "star5"
        ));
    }
}
