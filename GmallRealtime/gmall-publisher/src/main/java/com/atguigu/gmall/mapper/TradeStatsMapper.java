package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;

public interface TradeStatsMapper {

    // 某日总交易额
    @Select("select sum(order_amount) as order_amount from dws_trade_province_order_window partition par#{date}")
    BigDecimal selectGMV(Integer date);

    // 某日各省份交易额
    @Select("select province_name,sum(order_amount) as order_amount from dws_trade_province_order_window partition par#{date} group by province_name")
    List<TradeProvinceOrderAmount> selectProvinceAmount(Integer date);
}