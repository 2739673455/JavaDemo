package com.atguigu.edu.realtime.dws.test.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.function.DimAsyncFunction;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
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

public class DwsTestCoursePaperUserScoreDurationWindow extends BaseApp {
    public static void main(String[] args) {
        new DwsTestCoursePaperUserScoreDurationWindow().start(10110, 4, "dws_test_course_paper_score_duration_window", "dwd_test_exam");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
        SingleOutputStreamOperator<JSONObject> ds = kafkaStrDS.filter(o -> o != null && !o.trim().isEmpty())
                .map(JSON::parseObject);
        // 从 dim_test_paper 关联试卷名称，课程id
        SingleOutputStreamOperator<JSONObject> withCourseIdDS = AsyncDataStream.unorderedWait(ds,
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
                        in.put("course_id", dimJson.getString("course_id"));
                    }
                },
                60, TimeUnit.SECONDS);

        // 从 dim_course_info 关联课程名称
        SingleOutputStreamOperator<JSONObject> withCourseNameDS = AsyncDataStream.unorderedWait(withCourseIdDS,
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
        SingleOutputStreamOperator<JSONObject> result = withCourseNameDS.keyBy(o -> o.getString("user_id"))
                // 添加 试卷id和课程id 的 Map状态，若新到数据不被包含则添加人数计数；到0点清空状态
                .process(
                        new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                            MapState<String, Object> paperState;
                            MapState<String, Object> courseState;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                MapStateDescriptor<String, Object> stateDescriptor = new MapStateDescriptor<>("paperAndCourseState", String.class, Object.class);
                                paperState = getRuntimeContext().getMapState(stateDescriptor);
                                courseState = getRuntimeContext().getMapState(stateDescriptor);
                            }

                            @Override
                            public void processElement(JSONObject in, KeyedProcessFunction<String, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                                in.put("paper_user_count", 0L);
                                in.put("course_user_count", 0L);
                                in.put("view_count", 1L);
                                String paperId = in.getString("paper_id");
                                String courseId = in.getString("course_id");
                                if (!paperState.contains(paperId))
                                    in.put("paper_user_count", 1L);
                                if (!courseState.contains(courseId))
                                    in.put("course_user_count", 1L);
                                paperState.put(paperId, null);
                                courseState.put(courseId, null);
                                TimerService timerService = context.timerService();
                                // 注册下一天0点定时器
                                long nowTime = timerService.currentProcessingTime();
                                long timerTime = nowTime - (nowTime + 8 * 3600) % 86400 + 86400;
                                timerService.registerProcessingTimeTimer(timerTime);
                                collector.collect(in);
                            }

                            // 0点清空状态
                            @Override
                            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                                paperState.clear();
                                courseState.clear();
                            }
                        }
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofMillis(0L))
                                .withTimestampAssigner((o, l) -> o.getLong("ts"))
                )
                .keyBy(o -> o.getString("paper_id"))
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .reduce(
                        new ReduceFunction<JSONObject>() {
                            @Override
                            public JSONObject reduce(JSONObject result, JSONObject in) throws Exception {
                                result.put("paper_user_count", result.getLong("paper_user_count") + in.getLong("paper_user_count"));
                                result.put("course_user_count", result.getLong("course_user_count") + in.getLong("course_user_count"));
                                result.put("view_count", result.getLong("view_count") + in.getLong("view_count"));
                                result.put("score", result.getBigDecimal("score").add(in.getBigDecimal("score")));
                                result.put("duration_sec", result.getLong("duration_sec") + in.getLong("duration_sec"));
                                return result;
                            }
                        },
                        new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
                            @Override
                            public void process(String string, ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context context, Iterable<JSONObject> iterable, Collector<JSONObject> collector) throws Exception {
                                JSONObject next = iterable.iterator().next();
                                String start = DateFormatUtil.tsToDateTime(context.window().getStart());
                                String end = DateFormatUtil.tsToDateTime(context.window().getEnd());
                                next.put("start_time", start);
                                next.put("end_time", end);
                                next.put("current_date", DateFormatUtil.tsToDate(context.window().getStart()));
                                next.remove("user_id");
                                next.remove("id");
                                next.remove("ts");
                                collector.collect(next);
                            }
                        }
                );
        result.print();
        result.addSink(FlinkSinkUtil.getDorisSink(
                "dws_test_course_paper_score_duration_window",
                "start_time",
                "end_time",
                "current_date",
                "course_id",
                "course_name",
                "paper_id",
                "paper_title",
                "view_count",
                "course_user_count",
                "paper_user_count",
                "score",
                "duration_sec")
        );
    }
}
