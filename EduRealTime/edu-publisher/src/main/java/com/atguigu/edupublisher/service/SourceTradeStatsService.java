package com.atguigu.edupublisher.service;

import com.atguigu.edupublisher.bean.TradeSourceStats;

import java.util.List;

public interface SourceTradeStatsService {
    List<TradeSourceStats> getTradeSourceStats(Integer date);
}
