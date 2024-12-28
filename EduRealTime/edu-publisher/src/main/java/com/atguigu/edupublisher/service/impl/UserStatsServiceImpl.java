package com.atguigu.edupublisher.service.impl;

import com.atguigu.edupublisher.bean.UserFunnelStatsBean;
import com.atguigu.edupublisher.mapper.UserStatsMapper;
import com.atguigu.edupublisher.service.UserStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class UserStatsServiceImpl implements UserStatsService {
    @Autowired
    UserStatsMapper userStatsMapper;

    @Override
    public List<UserFunnelStatsBean> getUserFunnelStats(Integer date) {
        return userStatsMapper.selectUserFunnelStats(date);
    }
}
