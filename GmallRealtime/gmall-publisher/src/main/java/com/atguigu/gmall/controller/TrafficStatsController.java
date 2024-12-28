package com.atguigu.gmall.controller;

import com.atguigu.gmall.bean.TrafficUvCt;
import com.atguigu.gmall.service.TrafficStatsService;
import com.atguigu.gmall.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class TrafficStatsController {
    @Autowired
    private TrafficStatsService trafficStatsService;

    @RequestMapping("/ch_uv")
    public String chuv(@RequestParam(value = "date", defaultValue = "0") Integer date, @RequestParam(value = "limit", defaultValue = "10") Integer limit) {
        if (date == 0)
            date = DateFormatUtil.now();
        List<TrafficUvCt> chUvList = trafficStatsService.getChUvCt(date, limit);
        StringBuilder result = new StringBuilder();
        result.append("{\"status\": 0,\"data\": {\"columns\": [{\"id\": \"ch\",\"name\": \"渠道\"},{\"id\": \"uv\",\"name\": \"访客数\"}],\"rows\": [");
        for (TrafficUvCt o : chUvList) {
            result.append("{\"ch\": \"").append(o.getCh()).append("\",\"uv\": ").append(o.getUv()).append("},");
        }
        result.deleteCharAt(result.length() - 1);
        result.append("]}}");
        return result.toString();
    }
}
