package com.atguigu.edu.common.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class videoPlayBean {
	// 窗口起始时间
	String stt;

	// 窗口结束时间
	String edt;

	// 日期
	String cur_date;

	// 视频 ID
	String videoId;

	// 章节 ID
	String chapterId;

	// 章节名称
	String chapterName;

	// 设备id
	String minId;

	// 播放次数
	Long playCount;

	// 播放总时长
	Long playTotalSec;

	// 观看人数
	Long playUuCount;

	// 时间戳
	Long ts;

}
