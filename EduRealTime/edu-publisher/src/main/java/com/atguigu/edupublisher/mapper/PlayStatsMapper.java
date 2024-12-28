package com.atguigu.edupublisher.mapper;

import com.atguigu.edupublisher.bean.PlayStatsBean;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface PlayStatsMapper {
	//1.6.1 各章节播放统计
	@Select(
		"select\n" +
				"    chapter_id as chapterId,\n" +
				"    chapter_name as chapterName,\n" +
				"    sum(play_count) as playCount,\n" +
				"    sum(play_total_sec) as playTotalSec,\n" +
				"    sum(play_uu_count) as playUuCount,\n" +
				"    IFnull(sum(play_total_sec) / sum(play_uu_count),0)  as PerCapitaViewingTime\n" +
				"from edu.dws_chapter_video_play partition par20241115\n" +
				"group by chapter_id,chapter_name;"
	)
	List<PlayStatsBean> selectChapterPlay(Integer date);
}
