package com.atguigu.edupublisher.mapper;

import com.atguigu.edupublisher.bean.UserFunnelStatsBean;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface UserStatsMapper {
    //1.2.3 用户行为漏斗分析
    @Select("select    sum(home_count)          as homeCount,\n" +
            "          sum(course_detail_count) as courseDetailCount,\n" +
            "          sum(cart_add_count)      as cartAddCount,\n" +
            "          sum(order_count)         as orderCount,\n" +
            "          sum(pay_success_count)   as paySuccessCount\n" +
            "from      edu.dws_user_funnel_window partition par#{date};")
    List<UserFunnelStatsBean> selectUserFunnelStats(@Param("date") Integer date);
}
