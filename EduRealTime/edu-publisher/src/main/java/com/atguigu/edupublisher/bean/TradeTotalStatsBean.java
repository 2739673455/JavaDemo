package com.atguigu.edupublisher.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeTotalStatsBean {
    String currentDate;
    BigDecimal finalAmount;
    Long uv;
    Long vv;
}
