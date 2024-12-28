package com.atguigu.edu.common.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DwsTradeProvinceOrderWindowBean {
    // 窗口起始时间
    String stt;

    // 窗口结束时间
    String edt;

    // 日期
    String curDate;
    // 省份 ID
    String provinceId;

    //省份 名称
    @Builder.Default
    String provinceName = "" ;
    // 用户 ID
    String userId;

    // 订单总额
    BigDecimal orderTotalAmount;

    // 下单独立用户数
    Long orderUuCount;

    // 订单数
    Long orderCount;

    // 时间戳
    Long ts;

}
