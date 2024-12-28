package com.atguigu.edupublisher.controller;

import com.atguigu.edupublisher.bean.PlayStatsBean;
import com.atguigu.edupublisher.service.PlayStatsService;
import com.atguigu.edupublisher.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
public class PlayStatsController {
	@Autowired
	PlayStatsService playStatsService;

	@RequestMapping("/play_chapter_stats")
	public String playChapterStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
		if (date == 0){
			date = DateFormatUtil.now();
		}
		List<PlayStatsBean> result = playStatsService.getPlayStats(date);

		ArrayList<Object> chapterList = new ArrayList<>();
		ArrayList<Object> playCount = new ArrayList<>();
		ArrayList<Object> playTime = new ArrayList<>();
		ArrayList<Object> watchPeople = new ArrayList<>();
		ArrayList<Object> avgPlayTime = new ArrayList<>();



			for (PlayStatsBean psb : result) {
				chapterList.add(psb.getChapterName());
				playCount.add(psb.getPlayCount());
				playTime.add(psb.getPlayTotalSec());
				watchPeople.add(psb.getPlayUuCount());
				avgPlayTime.add(psb.getPerCapitaViewingTime());
			}


		String json = "{\"status\": 0,\"data\": { \n" +
				"\t\"categories\": [\""+StringUtils.join(chapterList,"\",\"")+"\"],\n" +
				"    \"series\": [{\"name\": \"视频播放次数\",\"data\": ["+StringUtils.join(playCount,",")+"]},\n" +
				"   \t\t\t   {\"name\": \"累计播放时长\",\"data\": ["+StringUtils.join(playTime,",")+"]},\n" +
				"      \t\t   {\"name\": \"观看人数\",\"data\": ["+StringUtils.join(watchPeople,",")+"]},\n" +
				"               {\"name\": \"人均观看时长\",\"data\": ["+StringUtils.join(avgPlayTime,",")+"]}]}}";

		return json;

	}
}
