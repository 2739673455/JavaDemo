package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.math.BigDecimal;

//各来源交易统计实体类
@Data
@AllArgsConstructor
public class TradeSourceStats {
    // 来源名称
    String sourceName;

    // 订单总额
    BigDecimal finalAmount;

    // 下单独立用户数
    Long userCount;

    // 订单数
    Long orderCount;
}
