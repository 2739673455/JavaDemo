package com.atguigu.edu.realtime.dws.order.situation;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.constant.Constant;
import com.atguigu.edu.common.function.DimAsyncFunction;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class DwsOrderSituation extends BaseApp {
	public static void main(String[] args) {
		new DwsOrderSituation().start(10045, 4, "dws_order_situation", Constant.TOPIC_DWD_TRADE_ORDER_DETAIL);
	}

	@Override
	public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
		//TODO 1.对流中数据进行转换,并过滤空消息
		SingleOutputStreamOperator<JSONObject> prDS = kafkaStrDS.process(
				new ProcessFunction<String, JSONObject>() {
					@Override
					public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
						if (StringUtils.isNotEmpty(value)) {
							JSONObject jsonObject = JSONObject.parseObject(value);
							out.collect(jsonObject);
						}
					}
				}
		);
		SingleOutputStreamOperator<JSONObject> courseDS = AsyncDataStream.unorderedWait(
				prDS,
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
						in.put("subject_id", dimJson.getString("subject_id"));
						in.put("course_name", dimJson.getString("course_name"));
					}
				},
				60,
				TimeUnit.SECONDS
		);
		SingleOutputStreamOperator<JSONObject> subDS = AsyncDataStream.unorderedWait(
				courseDS,
				new DimAsyncFunction<JSONObject>() {
					@Override
					public String getRowKey(JSONObject in) {
						return in.getString("subject_id");
					}

					@Override
					public String getTableName() {
						return "dim_base_subject_info";
					}

					@Override
					public void addDim(JSONObject in, JSONObject dimJson) {
						in.put("subject_name", dimJson.getString("subject_name"));
						in.put("category_id", dimJson.getString("category_id"));

					}
				},
				60,
				TimeUnit.SECONDS
		);
		SingleOutputStreamOperator<JSONObject> joinDS = AsyncDataStream.unorderedWait(
				subDS,
				new DimAsyncFunction<JSONObject>() {
					@Override
					public String getRowKey(JSONObject in) {
						return in.getString("category_id");
					}

					@Override
					public String getTableName() {
						return "dim_base_category_info";
					}

					@Override
					public void addDim(JSONObject in, JSONObject dimJson) {
						in.put("category_name", dimJson.getString("category_name"));
					}
				},
				60,
				TimeUnit.SECONDS
		);
		//joinDS.print();
		KeyedStream<JSONObject, String> userIdKey = joinDS.keyBy(
				new KeySelector<JSONObject, String>() {
					@Override
					public String getKey(JSONObject value) throws Exception {
						return value.getString("user_id");
					}
				}
		);
		SingleOutputStreamOperator<JSONObject> processDS = userIdKey.process(
				new ProcessFunction<JSONObject, JSONObject>() {
					MapState<String, Object> categoryState;
					MapState<String, Object> subjectState;
					MapState<String, Object> courseState;

					@Override
					public void open(Configuration parameters) throws Exception {
						MapStateDescriptor<String, Object> stateProperties = new MapStateDescriptor<String, Object>("cateSubState", String.class, Object.class);
						stateProperties.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
						categoryState = getRuntimeContext().getMapState(stateProperties);
						subjectState = getRuntimeContext().getMapState(stateProperties);
						courseState = getRuntimeContext().getMapState(stateProperties);
					}

					@Override
					public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
						value.put("course_order", 0L);
						value.put("category_order", 0L);
						value.put("subject_order", 0L);


						Long ts = value.getLong("ts");
						String date = DateFormatUtil.tsToDate(ts*1000);
						String categoryId = value.getString("category_id");
						String subjectId = value.getString("subject_id");
						String courseId = value.getString("course_id");
						if (!date.equals(courseState.get(courseId))) {
							value.put("course_order", 1L);
						}
						if (!date.equals(categoryState.get(categoryId))) {

							value.put("category_order", 1L);
						}
						if (!date.equals(subjectState.get(subjectId))) {
							value.put("subject_order", 1L);
						}
						courseState.put(categoryId, date);
						categoryState.put(categoryId, date);
						subjectState.put(subjectId, date);
						value.remove("session_id");
						value.remove("date_id");
						value.remove("out_trade_no");
						value.remove("coupon_reduce_amount");
						value.remove("origin_amount");
						value.remove("create_time");
						value.remove("row_op_ts");
						value.remove("trade_body");
						out.collect(value);

					}
				}
		);
		//processDS.print();
		SingleOutputStreamOperator<JSONObject> tsDS = processDS.assignTimestampsAndWatermarks(
				WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ZERO)
						.withTimestampAssigner(
								new SerializableTimestampAssigner<JSONObject>() {
									@Override
									public long extractTimestamp(JSONObject element, long recordTimestamp) {
										return element.getLong("ts")*1000;
									}
								}
						)
		);
		KeyedStream<JSONObject, String> keyedStream = tsDS.keyBy(
				new KeySelector<JSONObject, String>() {
					@Override
					public String getKey(JSONObject value) throws Exception {
						return value.getString("course_id");
					}
				}
		);

		//keyedStream.print();
		WindowedStream<JSONObject, String, TimeWindow> windowDS = keyedStream.window(
				TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(5))
		);

		SingleOutputStreamOperator<JSONObject> resDS = windowDS.reduce(
				new ReduceFunction<JSONObject>() {
					@Override
					public JSONObject reduce(JSONObject value1, JSONObject value2) throws Exception {
						value1.put("course_order", value1.getLong("course_order") + value2.getLong("course_order"));
						value1.put("category_order", value1.getLong("category_order") + value2.getLong("category_order"));
						value1.put("subject_order", value1.getLong("subject_order") + value2.getLong("subject_order"));
						value1.put("final_amount", value1.getBigDecimal("final_amount").add(value2.getBigDecimal("final_amount")));
						return value1;
					}
				},
				new ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>() {
					@Override
					public void process(String s, ProcessWindowFunction<JSONObject, JSONObject, String, TimeWindow>.Context ctx, Iterable<JSONObject> elements, Collector<JSONObject> out) throws Exception {
						JSONObject next = elements.iterator().next();
						String start = DateFormatUtil.tsToDateTime(ctx.window().getStart());
						String end = DateFormatUtil.tsToDateTime(ctx.window().getEnd());
						String current_date = DateFormatUtil.tsToDate(ctx.window().getStart());
						next.put("start_time", start);
						next.put("end_time", end);
						next.put("current_date", current_date);
						out.collect(next);
					}
				}
		);
		resDS.print();
		resDS.addSink(FlinkSinkUtil.getDorisSink(
			"dws_order_situation",
				"start_time",
				"end_time",
				"current_date",
				"course_id",
				"course_name",
				"category_id",
				"category_name",
				"subject_id",
				"subject_name",
				"course_order",
				"final_amount",
				"category_order",
				"subject_order"
		));
	}
}
