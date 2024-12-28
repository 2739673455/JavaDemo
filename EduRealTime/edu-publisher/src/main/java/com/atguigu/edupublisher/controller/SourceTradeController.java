package com.atguigu.edupublisher.controller;

import com.atguigu.edupublisher.bean.TradeSourceStats;
import com.atguigu.edupublisher.service.SourceTradeStatsService;
import com.atguigu.edupublisher.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class SourceTradeController {
    @Autowired
    private SourceTradeStatsService sourceTradeStatsService;

    @RequestMapping("source")
    public String getSourceStats(@RequestParam(value = "date",defaultValue = "0") Integer date){
        if (date == 0){
            date = DateFormatUtil.now();
        }
        List<TradeSourceStats> sourceOrderAmountList = sourceTradeStatsService.getTradeSourceStats(date);

        StringBuilder jsonB = new StringBuilder("{\"status\": 0,\"data\": {\"categories\": [");
        for (int i = 0; i < sourceOrderAmountList.size(); i++) {
            TradeSourceStats pa = sourceOrderAmountList.get(i);
            jsonB.append("\""+pa.getSourceName());
            if(i < sourceOrderAmountList.size() - 1){
                jsonB.append("\",");
                continue;
            }
                jsonB.append("\"],\"series\":[{ \"name\":\"下单总金额\",\"data\":[");
        }
        for (int i = 0; i < sourceOrderAmountList.size(); i++) {
            TradeSourceStats pa = sourceOrderAmountList.get(i);
            jsonB.append(pa.getFinalAmount());
            if(i < sourceOrderAmountList.size() - 1){
                jsonB.append(",");
                continue;
            }
                jsonB.append("]},{ \"name\":\"下单总次数\",\"data\":[");
        }
        for (int i = 0; i < sourceOrderAmountList.size(); i++) {
            TradeSourceStats pa = sourceOrderAmountList.get(i);
            jsonB.append(pa.getOrderCount());
            if(i < sourceOrderAmountList.size() - 1){
                jsonB.append(",");
                continue;
            }
                jsonB.append("]},{\"name\":\"下单用户数\",\"data\":[");
        }
        for (int i = 0; i < sourceOrderAmountList.size(); i++) {
            TradeSourceStats pa = sourceOrderAmountList.get(i);
            jsonB.append(pa.getUserCount());
            if(i < sourceOrderAmountList.size() - 1){
                jsonB.append(",");
            }
        }
        jsonB.append("]}]}}");
        return jsonB.toString();
    }


//        return "{\n" +
//                "  \"status\": 0,\n" +
//                "  \"msg\": \"\",\n" +
//                "  \"data\": {\n" +
//                "    \"columns\": [\n" +
//                "      {\n" +
//                "        \"name\": \"来源名称\",\n" +
//                "        \"id\": \"source_name\"\n" +
//                "      },\n" +
//                "      {\n" +
//                "        \"name\": \"订单总额\",\n" +
//                "        \"id\": \"final_amount\"\n" +
//                "      },\n" +
//                "      {\n" +
//                "        \"name\": \"下单用户总数\",\n" +
//                "        \"id\": \"user_count\"\n" +
//                "      },\n" +
//                "      {\n" +
//                "        \"name\": \"订单总数\",\n" +
//                "        \"id\": \"order_count\"\n" +
//                "      }\n" +
//                "    ],\n" +
//                "    \"rows\": \n" + rows +
//                "    \n" +
//                "  }\n" +
//                "}";
    }

