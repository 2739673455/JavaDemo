package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PlayStatsBean {
	String chapterId;
	String chapterName;
	Long playCount;
	Long playTotalSec;
	Long playUuCount;
	Long perCapitaViewingTime;

}
