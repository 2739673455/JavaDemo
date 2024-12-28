package com.atguigu.gmall.service;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;

import java.math.BigDecimal;
import java.util.List;

public interface TradeStatsService {

    BigDecimal getGMV(Integer date);

    List<TradeProvinceOrderAmount> getProvinceAmount(Integer date);
}
