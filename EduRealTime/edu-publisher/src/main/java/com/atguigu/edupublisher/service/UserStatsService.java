package com.atguigu.edupublisher.service;

import com.atguigu.edupublisher.bean.UserFunnelStatsBean;

import java.util.List;

public interface UserStatsService {
    List<UserFunnelStatsBean> getUserFunnelStats(Integer date);
}
