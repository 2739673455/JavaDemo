package com.atguigu.edu.realtime.dws.test.distribution.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.function.DimAsyncFunction;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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

public class DwsTestPaperScoreDistrubutionWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTestPaperScoreDistrubutionWindow().start(10113, 4, "dws-test-paper-score-distribution-window", "dwd_test_exam");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> ds = kafkaStrDS.filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject)
                .map(
                        (MapFunction<JSONObject, JSONObject>) in -> {
                            BigDecimal score = in.getBigDecimal("score");
                            BigDecimal[] bigDecimals = score.divideAndRemainder(BigDecimal.TEN);
                            BigDecimal scoreRangeLeft = bigDecimals[0].multiply(BigDecimal.TEN);
                            BigDecimal scoreRangeRigtt = scoreRangeLeft.add(BigDecimal.TEN);
                            in.put("score_range", scoreRangeLeft + "-" + scoreRangeRigtt);
                            if (scoreRangeLeft.equals(BigDecimal.valueOf(100L)))
                                in.put("score_range", scoreRangeLeft);
                            in.remove("score");
                            in.remove("id");
                            in.remove("duration_sec");
                            return in;
                        }
                )
                .keyBy(o -> o.getString("paper_id") + ":" + o.getString("score_range") + ":" + o.getString("user_id"))
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            ValueState<String> state;

                            @Override
                            public void open(Configuration parameters) {
                                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("date", String.class);
                                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                                state = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                in.put("uv", 0L);
                                String value = state.value();
                                String date = DateFormatUtil.tsToDate(in.getLong("ts"));
                                if (!date.equals(value)) {
                                    in.put("uv", 1L);
                                }
                                in.remove("user_id");
                                collector.collect(in);
                                state.update(date);
                            }
                        }
                )
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(0L))
                        .withTimestampAssigner((o, l) -> o.getLong("ts")))
                .keyBy(o -> o.getString("paper_id") + ":" + o.getString("score_range"))
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5L)))
                .reduce(
                        (ReduceFunction<JSONObject>) (result, in) -> {
                            result.put("uv", result.getLong("uv") + in.getLong("uv"));
                            return result;
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
        SingleOutputStreamOperator<JSONObject> withPaperTitleDS = AsyncDataStream.unorderedWait(ds,
                new DimAsyncFunction<JSONObject>() {
                    @Override
                    public String getRowKey(JSONObject in) {
                        return in.getString("paper_id");
                    }

                    @Override
                    public String getTableName() {
                        return "dim_test_paper";
                    }

                    @Override
                    public void addDim(JSONObject in, JSONObject dimJson) {
                        in.put("paper_title", dimJson.getString("paper_title"));
                    }
                },
                60, TimeUnit.SECONDS);
        withPaperTitleDS.print();
        withPaperTitleDS.addSink(FlinkSinkUtil.getDorisSink(
                "dws_test_paper_score_distribution_window",
                "start_time",
                "end_time",
                "current_date",
                "paper_id",
                "paper_title",
                "score_range",
                "uv"));
    }
}
