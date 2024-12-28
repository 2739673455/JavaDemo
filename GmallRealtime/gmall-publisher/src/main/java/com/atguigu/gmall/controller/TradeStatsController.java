package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.TradeProvinceOrderAmount;
import com.atguigu.gmall.service.TradeStatsService;
import com.atguigu.gmall.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.List;

@RestController
public class TradeStatsController {
    @Autowired
    TradeStatsService tradeStatsService;

    @RequestMapping("/gvm")
    public String gvm(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0)
            date = DateFormatUtil.now();
        BigDecimal gmv = tradeStatsService.getGMV(date);
        return "{\"status\":0,\"data\":" + gmv + "}";
    }

    @RequestMapping("/province_amount")
    public String provinceAmount(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0)
            date = DateFormatUtil.now();
        List<TradeProvinceOrderAmount> provinceAmountList = tradeStatsService.getProvinceAmount(date);
        StringBuilder result = new StringBuilder();
        result.append("{\"status\": 0,\"data\": {\"mapData\": [");
        for (TradeProvinceOrderAmount o : provinceAmountList) {
            result.append("{\"name\": \"").append(o.getProvinceName()).append("\",\"value\": ").append(o.getAmount()).append("},");
        }
        result.deleteCharAt(result.length() - 1);
        result.append("],\"valueName\": \"销售额\"}}");
        return result.toString();
    }
}