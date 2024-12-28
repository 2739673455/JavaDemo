package com.atguigu.edupublisher.mapper;

import com.atguigu.edupublisher.bean.TradeByProvinceStatsBean;
import com.atguigu.edupublisher.bean.TradeTotalStatsBean;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TradeStatsMapper {

    // 1.4.2 各省份交易统计
    @Select("select    province_name       as provinceName,\n" +
            "          sum(final_amount)   as finalAmount,\n" +
            "          sum(uu_order_count) as uv,\n" +
            "          sum(order_count)    as vv\n" +
            "from      edu.dws_trade_province_order_window partition par#{date}\n" +
            "group by  province_name;")
    List<TradeByProvinceStatsBean> selectTradeByProvinceStats(@Param("date") Integer date);

    //1.4.3 交易综合统计
    @Select("select    `current_date`    as currentDate,\n" +
            "          sum(final_amount) as finalAmount,\n" +
            "          sum(uv)           as uv,\n" +
            "          sum(vv)           as vv\n" +
            "from      edu.dws_trade_total_window\n" +
            "where     `current_date` between date_sub(#{date}, 3) and #{date}\n" +
            "group by  `current_date`;")
    List<TradeTotalStatsBean> selectTradeTotalStats(@Param("date") Integer date);
}
