package com.atguigu.edupublisher.service.impl;

import com.atguigu.edupublisher.bean.*;
import com.atguigu.edupublisher.mapper.TrafficStatsMapper;
import com.atguigu.edupublisher.service.TrafficStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {
    @Autowired
    TrafficStatsMapper trafficStatsMapper;


    @Override
    public List<TrafficSvCt> getSvCt(Integer date) {
        return trafficStatsMapper.selectSvCt(date);
    }

    @Override
    public List<TrafficPvPerSession> getPvPerSession(Integer date) {
        return trafficStatsMapper.selectPvPerSession(date);
    }

    @Override
    public List<TrafficDurPerSession> getDurPerSession(Integer date) {
        return trafficStatsMapper.selectDurPerSession(date);
    }

    @Override
    public List<TrafficUjRate> getUjRate(Integer date) {
        return trafficStatsMapper.selectUjRate(date);
    }

    @Override
    public List<TrafficUvCt> getUvCt(Integer date) {
        return trafficStatsMapper.selectUvCt(date);
    }

    @Override
    public List<TrafficNewAndOldVisitorTraffic> getTrafficNewAndOldVisitorTraffic(Integer date) {
        return trafficStatsMapper.selectNewAndOldVisitor(date);
    }

    @Override
    public List<TrafficKeywordStatsBean> getKeywordStats(Integer date) {
        return trafficStatsMapper.selectKeywordStats(date);
    }
}
