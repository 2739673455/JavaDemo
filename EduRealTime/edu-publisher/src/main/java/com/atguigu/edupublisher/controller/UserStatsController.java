package com.atguigu.edupublisher.controller;

import com.atguigu.edupublisher.bean.UserFunnelStatsBean;
import com.atguigu.edupublisher.service.UserStatsService;
import com.atguigu.edupublisher.util.DateFormatUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class UserStatsController {
    @Autowired
    UserStatsService userStatsService;

    @RequestMapping("/user_funnel_stats")
    public String userFunnelStats(@RequestParam(value = "date", defaultValue = "0") Integer date) {
        if (date == 0)
            date = DateFormatUtil.now();
        List<UserFunnelStatsBean> beans = userStatsService.getUserFunnelStats(date);
        StringBuilder result = new StringBuilder();
        result.append("{\"status\": 0,\"data\": [");
        for (UserFunnelStatsBean bean : beans) {
            result.append(String.format("{\"name\": \"首页浏览人数\",\"value\": %s},", bean.getHomeCount()));
            result.append(String.format("{\"name\": \"商品详情页浏览人数\",\"value\": %s},", bean.getCourseDetailCount()));
            result.append(String.format("{\"name\": \"加购人数\",\"value\": %s},", bean.getCartAddCount()));
            result.append(String.format("{\"name\": \"下单人数\",\"value\": %s},", bean.getOrderCount()));
            result.append(String.format("{\"name\": \"支付成功人数\",\"value\": %s},", bean.getPaySuccessCount()));
        }
        result.deleteCharAt(result.length() - 1);
        result.append("]}");
        return result.toString();
    }
}
