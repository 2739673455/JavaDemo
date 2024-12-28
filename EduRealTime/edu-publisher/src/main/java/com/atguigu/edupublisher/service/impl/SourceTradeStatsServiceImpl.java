package com.atguigu.edupublisher.service.impl;

import com.atguigu.edupublisher.bean.TradeSourceStats;
import com.atguigu.edupublisher.mapper.SourceTradeStatsMapper;
import com.atguigu.edupublisher.service.SourceTradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SourceTradeStatsServiceImpl implements SourceTradeStatsService {
    @Autowired
    private SourceTradeStatsMapper sourceTradeStatsMapper;


    @Override
    public List<TradeSourceStats> getTradeSourceStats(Integer date) {
        return sourceTradeStatsMapper.selectTradeSourceStats(date);
    }
//
//    @Override
//    public BigDecimal selectFinalAmount(Integer date) {
//        return sourceTradeStatsMapper.selectFinalAmount(date);
//    }
//
//    @Override
//    public Long selectOrderCount(Integer date) {
//        return sourceTradeStatsMapper.selectOrderCount(date);
//    }


}


