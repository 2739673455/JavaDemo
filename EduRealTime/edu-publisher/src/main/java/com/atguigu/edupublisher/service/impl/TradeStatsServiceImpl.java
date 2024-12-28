package com.atguigu.edupublisher.service.impl;

import com.atguigu.edupublisher.bean.TradeByProvinceStatsBean;
import com.atguigu.edupublisher.bean.TradeTotalStatsBean;
import com.atguigu.edupublisher.mapper.TradeStatsMapper;
import com.atguigu.edupublisher.service.TradeStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TradeStatsServiceImpl implements TradeStatsService {
    @Autowired
    TradeStatsMapper tradeStatsMapper;

    @Override
    public List<TradeTotalStatsBean> getTradeTotalStats(Integer date) {
        return tradeStatsMapper.selectTradeTotalStats(date);
    }

    @Override
    public List<TradeByProvinceStatsBean> getTradeByProvinceStats(Integer date) {
        return tradeStatsMapper.selectTradeByProvinceStats(date);
    }
}
