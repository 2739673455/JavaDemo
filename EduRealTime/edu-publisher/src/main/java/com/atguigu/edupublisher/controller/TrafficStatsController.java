package com.atguigu.edupublisher.controller;

import com.atguigu.edupublisher.bean.*;
import com.atguigu.edupublisher.service.TrafficStatsService;
import com.atguigu.edupublisher.util.DateFormatUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
public class TrafficStatsController {
    @Autowired
    TrafficStatsService trafficStatsService;

    @RequestMapping("/traffic_sc_uvct")
    public String getUvCt(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficUvCt> beans = trafficStatsService.getUvCt(date);
        List scNameList = new ArrayList();
        List uvCtList = new ArrayList();

        for (TrafficUvCt bean : beans) {
            scNameList.add(bean.getSc());
            uvCtList.add(bean.getUvCt());
        }

        String json = "{\"status\": 0,\n" +
                "  \"data\": {\"categories\": [\""+StringUtils.join(scNameList,"\",\"")+"\"],\n" +
                "    \"series\": [{\"name\": \"独立访客数\",\n" +
                "                \"data\": ["+StringUtils.join(uvCtList,",")+"]}]}}";
//        String json = "{\n" +
//                "  \"status\": 0,\n" +
//                "  \"msg\": \"\",\n" +
//                "  \"data\": {\"categories\": [\""+StringUtils.join(scNameList,"\",\"")+"\"],\n" +
//                "    \"series\": [{\"name\": \"独立访客数\",\"data\": ["+StringUtils.join(uvCtList,",")+"}]}\n" +
//                "}";
//        Map resMap = new HashMap();
//        resMap.put("status",0);
//        resMap.put("msg","");
//        Map dataMap = new HashMap();
//        List scNameList = new ArrayList();
//        List seriesList = new ArrayList();
//        List uvCtList = new ArrayList();
//        for (TrafficUvCt bean : beans) {
//            scNameList.add(bean.getSc());
//            uvCtList.add(bean.getUvCt());
//        }
//        Map uvCtMap = new HashMap();
//        uvCtMap.put("name","独立访客数");
//        uvCtMap.put("data",uvCtList);
//        seriesList.add(uvCtMap);
//        dataMap.put("categories",scNameList);
//        dataMap.put("series",seriesList);
//        return resMap;
        return json;
    }

    @RequestMapping("/traffic_sc_svct")
    public String getPvCt(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficSvCt> beans = trafficStatsService.getSvCt(date);
        List scNameList = new ArrayList();
        List svCtList = new ArrayList();

        for (TrafficSvCt bean : beans) {
            scNameList.add(bean.getSc());
            svCtList.add(bean.getSvCt());
        }



//        String json = "{\n" +
//                "  \"status\": 0,\n" +
//                "  \"data\": {\n" +
//                "    \"options\": [\n" +
//                "      {\n" +
//                "        \"xAxis\": [\n" +
//                "          {\n" +
//                "            \"data\": [\""+StringUtils.join(scNameList,"\",\"")+"\"]\n" +
//                "          }\n" +
//                "        ],\n" +
//                "        \"series\": [\n" +
//                "          {\n" +
//                "            \"name\": \"income\",\n" +
//                "            \"data\": ["+StringUtils.join(svCtList,)+"],\n" +
//                "            \"type\": \"bar\"\n" +
//                "          }\n" +
//                "        ]\n" +
//                "      }\n" +
//                "    ],\n" +
//                "    \"timeline\": [\n" +
//                "      20241124\n" +
//                "    ]\n" +
//                "  }\n" +
//                "}";
        String json ="{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\": [\""+StringUtils.join(scNameList,"\",\"")+"\"],\n" +
                "    \"series\": [{\"name\": \"会话数\",\"data\": ["+StringUtils.join(svCtList,",")+"]}\n" +
                "    ]\n" +
                "  }\n" +
                "}";
        return json;
    }

    @RequestMapping("/traffic_sc_pvperSession")
    public String getPvPerSession(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficPvPerSession> beans
                = trafficStatsService.getPvPerSession(date);

        List scNameList = new ArrayList();
        List svPpsList = new ArrayList();

        for (TrafficPvPerSession bean : beans) {
            scNameList.add(bean.getSc());
            svPpsList.add(bean.getPvPerSession());
        }

        String json ="{\n" +
                "  \"status\": 0,\n" +
                "  \"msg\": \"\",\n" +
                "  \"data\": {\n" +
                "    \"categories\": [\""+StringUtils.join(scNameList,"\",\"")+"\"],\n" +
                "    \"series\": [{\"name\": \"会话数\",\"data\": ["+StringUtils.join(svPpsList,",")+"]}\n" +
                "    ]\n" +
                "  }\n" +
                "}";
        return json;
    }

    @RequestMapping("/traffic_sc_durperSession")
    public String TrafficSvCt(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficDurPerSession> beans = trafficStatsService.getDurPerSession(date);
        List scNameList = new ArrayList();
        List dpList = new ArrayList();

        for (TrafficDurPerSession bean : beans) {
            scNameList.add(bean.getSc());
            dpList.add(bean.getDurPerSession());
        }

        String json = "{\n" +
                "  \"status\": 0,\n" +
                "  \"data\": {\n" +
                "    \"options\": [\n" +
                "      {\n" +
                "        \"xAxis\": [\n" +
                "          {\n" +
                "            \"data\": [\"" + StringUtils.join(scNameList, "\",\"") + "\"]\n" +
                "          }\n" +
                "        ],\n" +
                "        \"series\": [\n" +
                "          {\n" +
                "            \"name\": \"income\",\n" +
                "            \"data\": [" + StringUtils.join(dpList, ",") + "],\n" +
                "            \"type\": \"bar\"\n" +
                "          }\n" +
                "        ]\n" +
                "      }\n" +
                "    ],\n" +
                "    \"timeline\": [\n" +
                "      20241124\n" +
                "    ]\n" +
                "  }\n" +
                "}";
        return json;
    }

    @RequestMapping("/traffic_sc_ujrate")
    public String TrafficUjRate(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficUjRate> result = trafficStatsService.getUjRate(date);
        return result.toString();
    }

    @RequestMapping("/traffic_new_and_old_visitor")
    public String trafficNewAndOldVisitor(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficNewAndOldVisitorTraffic> result = trafficStatsService.getTrafficNewAndOldVisitorTraffic(date);
        return result.toString();
    }

    @RequestMapping("/traffic_keyword_stats")
    public String trafficKeywordStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0) {
            date = DateFormatUtil.now();
        }
        List<TrafficKeywordStatsBean> beans = trafficStatsService.getKeywordStats(date);
        StringBuilder result = new StringBuilder();
        result.append("{\"status\": 0,\"data\": [");
        for (TrafficKeywordStatsBean bean : beans) {
            result.append("{\"name\": \"").append(bean.getKeyword()).append("\",\"value\": ").append(bean.getKeywordCount()).append("},");
        }
        result.deleteCharAt(result.length() - 1);
        result.append("]}");
        return result.toString();
    }
}
