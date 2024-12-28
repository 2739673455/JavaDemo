package com.atguigu.edupublisher.service;

import com.atguigu.edupublisher.bean.TradeByProvinceStatsBean;
import com.atguigu.edupublisher.bean.TradeTotalStatsBean;

import java.util.List;

public interface TradeStatsService {

    List<TradeTotalStatsBean> getTradeTotalStats(Integer date);

    List<TradeByProvinceStatsBean> getTradeByProvinceStats(Integer date);
}
