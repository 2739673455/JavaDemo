package com.atguigu.gmall.realtime.dwd.db.app;

import com.atguigu.edu.common.base.BaseSQLApp;
import com.atguigu.edu.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 视频播放事实表
 */
public class DwdTradeVideoPlay extends BaseSQLApp {
	public static void main(String[] args) {
		new DwdTradeVideoPlay().start(10033,4,"dwd_Trade_video_olay");
	}

	@Override
	public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {
		//TODO 1.设置状态失效时间
		streamTableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30));
		//TODO 2.在kafka的dwd_traffic_appVideo中去取播放数据
		streamTableEnv.executeSql(
				"create table dwd_traffic_appVideo(\n" +
						"    `appVideo` map<string,string>,\n" +
						"    `common` map<string,string>,\n" +
						"    `ts` bigint\n" +
						")\n" + SQLUtil.getKafkaSource("dwd_traffic_appVideo","dwd_Trade_video_olay")
		);
		Table logData = streamTableEnv.sqlQuery(
				"select\n" +
						"    `appVideo`['play_sec'] play_sec,\n" +
						"    `appVideo`['position_sec'] position_sec,\n" +
						"    `appVideo`['video_id'] video_id,\n" +
						"    `common`['mid'] mid,\n" +
						"    ts\n" +
						"from dwd_traffic_appVideo"
		);
		//TODO 3.放到kafka中
		streamTableEnv.executeSql(
				"create table dwd_Trade_video_olay_info(\n" +
						"    `play_sec` STRING,\n" +
						"    `position_sec` STRING,\n" +
						"    `video_id` string,\n" +
						"    `mid` string,\n" +
						"    ts bigint,\n" +
						"    primary key (`video_id`) not enforced\n" +
						")"+SQLUtil.getUpsertKafkaSink("dwd_Trade_video_olay_info")
		);
		//写入
		logData.executeInsert("dwd_Trade_video_olay_info");


	}
}
