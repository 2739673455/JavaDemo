package com.atguigu.edu.realtime.dws.video.play.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.base.BaseApp;
import com.atguigu.edu.common.bean.videoPlayBean;
import com.atguigu.edu.common.function.BeanToJsonStrMapFunction;
import com.atguigu.edu.common.function.DimAsyncFunction;
import com.atguigu.edu.common.util.DateFormatUtil;
import com.atguigu.edu.common.util.FlinkSinkUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 各章节播放统计
 */
public class DwsChapterVideoPlay extends BaseApp {
	public static void main(String[] args) {
		new DwsChapterVideoPlay().start(10034, 4, "dws_Chapter_Video_play", "dwd_Trade_video_olay_info");
	}

	@Override
	public void handle(StreamExecutionEnvironment env, DataStreamSource<String> kafkaStrDS) {
		//TODO 1.对流中的数据进行类型转换 并过滤空消息 JsonStr->JsonObj
		SingleOutputStreamOperator<JSONObject> processDS = kafkaStrDS.process(
				new ProcessFunction<String, JSONObject>() {
					@Override
					public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
						if (StringUtils.isNotEmpty(jsonStr)) {
							JSONObject jsonObj = JSON.parseObject(jsonStr);
							out.collect(jsonObj);
						}
					}
				}
		);
		//{"play_sec":"30","position_sec":"30","mid":"mid_158","video_id":"1982","ts":1731639401843}
		SingleOutputStreamOperator<JSONObject> withVideoDS = AsyncDataStream.unorderedWait(
				processDS,
				new DimAsyncFunction<JSONObject>() {

					@Override
					public String getRowKey(JSONObject in) {
						return in.getString("video_id");
					}

					@Override
					public String getTableName() {
						return "dim_video_info";
					}

					@Override
					public void addDim(JSONObject in, JSONObject dimJson) {
						//System.out.println(dimJson.getString("chapter_id"));
						in.put("chapter_id", dimJson.getString("chapter_id"));

					}
				},
				60,
				TimeUnit.SECONDS
		);


		//{"play_sec":"30","position_sec":"600","mid":"mid_80","chapter_id":"25447","video_id":"4552","ts":1731641389171}
		SingleOutputStreamOperator<JSONObject> withChapterDS = AsyncDataStream.unorderedWait(
				withVideoDS,
				new DimAsyncFunction<JSONObject>() {
					@Override
					public String getRowKey(JSONObject in) {
						return in.getString("chapter_id");
					}

					@Override
					public String getTableName() {
						return "dim_chapter_info";
					}

					@Override
					public void addDim(JSONObject in, JSONObject dimJson) {
						in.put("chapter_name", dimJson.getString("chapter_name"));
						in.put("course_id", dimJson.getString("course_id"));
					}
				},
				60,
				TimeUnit.SECONDS
		);
		/*
		{"course_id":"443","play_sec":"30","position_sec":"390","mid":"mid_111","chapter_name":"167_尚硅谷Vue3技术_Suspense组件","chapter_id":"21171","video_id":"276","ts":1731650433166}
		{"course_id":"443","play_sec":"24","position_sec":"600","mid":"mid_111","chapter_name":"167_尚硅谷Vue3技术_Suspense组件","chapter_id":"21171","video_id":"276","ts":1731650433166}
		{"course_id":"443","play_sec":"30","position_sec":"120","mid":"mid_111","chapter_name":"167_尚硅谷Vue3技术_Suspense组件","chapter_id":"21171","video_id":"276","ts":1731650433166}
		 */

		SingleOutputStreamOperator<videoPlayBean> mapDS = withChapterDS.map(
				new MapFunction<JSONObject, videoPlayBean>() {
					@Override
					public videoPlayBean map(JSONObject value) throws Exception {
						return videoPlayBean.builder()
								.videoId(value.getString("video_id"))
								.chapterId(value.getString("chapter_id"))
								.chapterName(value.getString("chapter_name"))
								.minId(value.getString("mid"))
								.playTotalSec(value.getLong("position_sec"))
								.playCount(1L)
								.ts(value.getLong("ts"))
								.build();
					}
				}
		);
		//mapDS.print();
		//TODO 2.根据Mid分组
		KeyedStream<videoPlayBean, String> midKeyDS = mapDS.keyBy(
				new KeySelector<videoPlayBean, String>() {
					@Override
					public String getKey(videoPlayBean value) throws Exception {
						return value.getMinId();
					}
				}
		);
		//TODO 3.过滤独立用户
		SingleOutputStreamOperator<videoPlayBean> BeanDS = midKeyDS.process(
				new KeyedProcessFunction<String, videoPlayBean, videoPlayBean>() {
					ValueState<String> lastPlayDtStats;

					@Override
					public void open(Configuration parameters) throws Exception {
						ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<String>("last_play_dt_stats", String.class);
						valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
						lastPlayDtStats = getRuntimeContext().getState(valueStateDescriptor);
					}

					@Override
					public void processElement(videoPlayBean bean, KeyedProcessFunction<String, videoPlayBean, videoPlayBean>.Context ctx, Collector<videoPlayBean> out) throws Exception {
						String lastDt = lastPlayDtStats.value();
						String curDt = DateFormatUtil.tsToDate(bean.getTs());
						if (lastDt == null || lastDt.compareTo(curDt) > 0) {
							bean.setPlayUuCount(1L);
							lastPlayDtStats.update(curDt);
						} else {
							bean.setPlayUuCount(0L);
						}
						out.collect(bean);
					}
				}
		);
		//BeanDS.print();
		//TODO 4.增加水位线
		SingleOutputStreamOperator<videoPlayBean> assignDS = BeanDS.assignTimestampsAndWatermarks(
				WatermarkStrategy.<videoPlayBean>forBoundedOutOfOrderness(Duration.ZERO)
						.withTimestampAssigner(
								new SerializableTimestampAssigner<videoPlayBean>() {
									@Override
									public long extractTimestamp(videoPlayBean element, long recordTimestamp) {
										return element.getTs();
									}
								}
						)
		);
		//TODO 5. 分组开窗聚合
		SingleOutputStreamOperator<videoPlayBean> reduceStream = assignDS.keyBy(videoPlayBean::getVideoId)
				.window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)))
				.reduce(new ReduceFunction<videoPlayBean>() {
					@Override
					public videoPlayBean reduce(videoPlayBean value1, videoPlayBean value2) throws Exception {
						// 相同组的数据度量值累加
						value1.setPlayCount(value1.getPlayCount() + value2.getPlayCount());
						value1.setPlayTotalSec(value1.getPlayTotalSec() + value2.getPlayTotalSec());
						value1.setPlayUuCount(value1.getPlayUuCount() + value2.getPlayUuCount());
						return value1;
					}
				}, new ProcessWindowFunction<videoPlayBean, videoPlayBean, String, TimeWindow>() {

					@Override
					public void process(String s, ProcessWindowFunction<videoPlayBean, videoPlayBean, String, TimeWindow>.Context context, Iterable<videoPlayBean> iterable, Collector<videoPlayBean> collector) throws Exception {
						String stt = DateFormatUtil.tsToDateTime(context.window().getStart());
						String edt = DateFormatUtil.tsToDateTime(context.window().getEnd());
						String cur = DateFormatUtil.tsToDate(context.window().getStart());
						for (videoPlayBean element : iterable) {
							element.setStt(stt);
							element.setEdt(edt);
							element.setCur_date(cur);
							element.setTs(System.currentTimeMillis());
							collector.collect(element);
						}
					}
				});
		SingleOutputStreamOperator<JSONObject> resultDS = reduceStream.map(new BeanToJsonStrMapFunction<>())
				.map(new MapFunction<String, JSONObject>() {
					@Override
					public JSONObject map(String value) throws Exception {
						return JSON.parseObject(value);
					}
				});
		resultDS.print();
		resultDS.addSink(FlinkSinkUtil.getDorisSink(
				"dws_chapter_video_play",
				"stt",
				"edt",
				"cur_date",
				"video_id",
				"chapter_id",
				"chapter_name",
				"min_id",
				"play_count",
				"play_total_sec",
				"play_uu_count"
		));
	}
	//1> {"cur_date":"2024-11-15",
	// "stt":"2024-11-15 16:57:00",
	// "min_id":"mid_356",
	// "edt":"2024-11-15 16:57:10",
	// "play_uu_count":1,
	// "chapter_name":"178_尚硅谷_MySQL基础_小结",
	// "chapter_id":"24289",
	// "play_count":36,
	// "play_total_sec":10260,
	// "ts":1731661119580,
	// "video_id":"3394"}
	//1> {"cur_date":"2024-11-15","stt":"2024-11-15 16:57:00","min_id":"mid_100","edt":"2024-11-15 16:57:10","play_uu_count":1,"chapter_name":"300_尚硅谷_中午演唱会_班主任","chapter_id":"22334","play_count":56,"play_total_sec":24360,"ts":1731661119755,"video_id":"1439"}
}
