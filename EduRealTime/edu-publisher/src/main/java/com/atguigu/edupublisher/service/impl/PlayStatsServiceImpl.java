package com.atguigu.edupublisher.service.impl;

import com.atguigu.edupublisher.bean.PlayStatsBean;
import com.atguigu.edupublisher.mapper.PlayStatsMapper;
import com.atguigu.edupublisher.service.PlayStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class PlayStatsServiceImpl implements PlayStatsService {

	@Autowired
	PlayStatsMapper playStatsMapper;

	@Override
	public List<PlayStatsBean> getPlayStats(Integer date) {
		return playStatsMapper.selectChapterPlay(date);
	}
}
