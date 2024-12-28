package com.atguigu.edu.realtime.dws.question.app;

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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsQuestionCorrectUuWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsQuestionCorrectUuWindow().start(10112, 4, "dws_test_question_correct_uu_window", "dwd_test_exam_question");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> ds = kafkaStrDS.filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .keyBy(o -> o.getString("question_id") + ":" + o.getString("user_id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            ValueState<String> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<String>("state", String.class);
                                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                state = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                String value = state.value();
                                String date = DateFormatUtil.tsToDate(in.getLong("ts"));
                                in.put("correct_count", 1L);
                                in.put("count", 1L);
                                in.put("correct_uu_count", 1L);
                                in.put("uu_count", 1L);
                                if (date.equals(value)) {
                                    in.put("uu_count", 0L);
                                    in.put("correct_uu_count", 0L);
                                }
                                if (in.getBigDecimal("score").compareTo(new BigDecimal("0")) == 0) {
                                    in.put("correct_count", 0L);
                                    in.put("correct_uu_count", 0L);
                                }
                                state.update(date);
                                in.remove("create_time");
                                in.remove("score");
                                in.remove("user_id");
                                in.remove("id");
                                collector.collect(in);
                            }
                        }
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((o, l) -> o.getLong("ts"))
                )
                .keyBy(o -> o.getString("question_id"))
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
                .reduce(
                        new ReduceFunction<JSONObject>() {
                            @Override
                            public JSONObject reduce(JSONObject result, JSONObject in) throws Exception {
                                result.put("count", result.getLong("count") + in.getLong("count"));
                                result.put("correct_count", result.getLong("correct_count") + in.getLong("correct_count"));
                                result.put("uu_count", result.getLong("uu_count") + in.getLong("uu_count"));
                                result.put("correct_uu_count", result.getLong("correct_uu_count") + in.getLong("correct_uu_count"));
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
                        }
                );
        SingleOutputStreamOperator<JSONObject> withQuestionTxtDS = AsyncDataStream.unorderedWait(ds,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public String getRowKey(JSONObject in) {
                        return in.getString("question_id");
                    }

                    @Override
                    public String getTableName() {
                        return "dim_test_question_info";
                    }

                    @Override
                    public void addDim(JSONObject in, JSONObject dimJson) {
                        in.put("question_txt", dimJson.getString("question_txt"));
                    }
                }, 60, TimeUnit.SECONDS
        );
        withQuestionTxtDS.print();
        withQuestionTxtDS.addSink(FlinkSinkUtil.getDorisSink(
                "dws_test_question_correct_uu_window",
                "start_time",
                "end_time",
                "current_date",
                "question_id",
                "question_txt",
                "count",
                "correct_count",
                "uu_count",
                "correct_uu_count"));
    }
}
