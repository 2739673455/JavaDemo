package com.atguigu.edupublisher.service;

import com.atguigu.edupublisher.bean.PlayStatsBean;

import java.util.List;

public interface PlayStatsService {
	List<PlayStatsBean> getPlayStats(Integer date);
}
