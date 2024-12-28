package com.atguigu.edupublisher.mapper;

import com.atguigu.edupublisher.bean.TradeSourceStats;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

//交易域来源统计mapper接口
public interface SourceTradeStatsMapper {

    @Select("select\n" +
            "           source_name,\n" +
            "           sum(final_amount)              as final_amount,\n" +
            "           sum(user_count)                as user_count,\n" +
            "           sum(order_count)               as order_count\n" +
            "from       edu.dws_trade_source_order_window\n" +
            "partition  par#{date}\n" +
            "group by   stt,source_id,source_name\n" +
            "/*ORDER BY   final_amount desc*/;")
    List<TradeSourceStats> selectTradeSourceStats(@Param("date") Integer date);

//
//    // 交易总金额
//    @Select("select sum(final_amount) as final_amount\n" +
//            "from dws_trade_source_order_window\n" +
//            "where toYYYYMMDD(stt) = #{date};")
//    BigDecimal selectFinalAmount(@Param("date") Integer date);
//
//    // 订单总数
//    @Select("select sum(order_count) as order_count\n" +
//            "from dws_trade_source_order_window\n" +
//            "where toYYYYMMDD(stt) = #{date};")
//    Long selectOrderCount(@Param("date") Integer date);
}
