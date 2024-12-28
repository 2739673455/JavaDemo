package com.atguigu.edupublisher.controller;

import com.atguigu.edupublisher.bean.TradeByProvinceStatsBean;
import com.atguigu.edupublisher.bean.TradeTotalStatsBean;
import com.atguigu.edupublisher.service.TradeStatsService;
import com.atguigu.edupublisher.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TradeStatsController {
    @Autowired
    TradeStatsService tradeStatsService;

    @RequestMapping("/trade_total_stats")
    public String tradeTotalStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0)
            date = DateFormatUtil.now();
        List<TradeTotalStatsBean> beans = tradeStatsService.getTradeTotalStats(date);
        StringBuilder result = new StringBuilder("{\"status\": 0,\"data\": {");
        StringBuilder currentDateStr = new StringBuilder().append("\"categories\": [");
        StringBuilder finalAmountStr = new StringBuilder("{\"name\": \"交易额\",\"data\": [");
        StringBuilder uvStr = new StringBuilder("{\"name\": \"交易人数\",\"data\": [");
        StringBuilder vvStr = new StringBuilder("{\"name\": \"交易次数\",\"data\": [");
        for (TradeTotalStatsBean bean : beans) {
            currentDateStr.append("\"").append(bean.getCurrentDate()).append("\",");
            finalAmountStr.append(bean.getFinalAmount()).append(",");
            uvStr.append(bean.getUv()).append(",");
            vvStr.append(bean.getVv()).append(",");
        }
        currentDateStr.deleteCharAt(currentDateStr.length() - 1);
        finalAmountStr.deleteCharAt(finalAmountStr.length() - 1);
        uvStr.deleteCharAt(uvStr.length() - 1);
        vvStr.deleteCharAt(vvStr.length() - 1);
        result.append(currentDateStr).append("],")
                .append("\"series\": [")
                .append(finalAmountStr).append("],\"type\": \"line\",\"yAxisIndex\": 0},")
                .append(uvStr).append("],\"type\": \"bar\",\"yAxisIndex\": 1},")
                .append(vvStr).append("],\"type\": \"bar\",\"yAxisIndex\": 1}")
                .append("]}}");
        return result.toString();
    }

    @RequestMapping("/trade_province_stats")
    public String tradeByProvinceStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0)
            date = DateFormatUtil.now();
        List<TradeByProvinceStatsBean> beans = tradeStatsService.getTradeByProvinceStats(date);
        StringBuilder result = new StringBuilder("{\"status\": 0,\"data\": {\"mapData\": [");
        for (TradeByProvinceStatsBean bean : beans) {
            result.append(String.format("{\"name\": \"%s\",\"sizeValue\": %f,\"tooltipValues\": [%d,%d]},",
                    bean.getProvinceName(), bean.getFinalAmount(), bean.getUv(), bean.getVv()));
        }
        result.deleteCharAt(result.length() - 1);
        result.append("],\"sizeValueName\": \"销售额\",\"tooltipNames\": [\"下单人数\",\"下单次数\"],\"tooltipUnits\": [\"人\",\"次\"]}}");
        return result.toString();
    }
}
