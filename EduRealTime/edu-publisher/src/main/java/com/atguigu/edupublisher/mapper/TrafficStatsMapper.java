package com.atguigu.edupublisher.mapper;

import com.atguigu.edupublisher.bean.*;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

@Mapper
public interface TrafficStatsMapper {
    // 1. 获取各来源独立访客数
    @Select("select sc_name,\n" +
            "       sum(uv_ct) uv_ct\n" +
            "from dws_traffic_sc_is_new_page_view_window\n" +
            "partition par#{date}\n" +
            "group by sc_name\n" +
            "order by uv_ct desc;")
    List<TrafficUvCt> selectUvCt(@Param("date") Integer date);

    //1.1.3 新老访客流量统计
    @Select("SELECT\n" +
            "    SUM(uv_ct) as uvCt,\n" +
            "    SUM(pv_ct) as pvCt,\n" +
            "    CAST(SUM(dur_sum) / SUM(uv_ct) as DECIMAL(10,2)) as avgOnLineTime,\n" +
            "    CAST(SUM(pv_ct) / SUM(uv_ct)as DECIMAL(10,2)) as  avgVisitPageNumber\n" +
            "FROM edu.dws_traffic_sc_is_new_page_view_window partition par#{date}\n" +
            "GROUP BY is_new")
    List<TrafficNewAndOldVisitorTraffic> selectNewAndOldVisitor(Integer date);

    //1.1.4 关键词统计
    @Select("select    keyword,\n" +
            "          sum(keyword_count) as keywordCount\n" +
            "from      edu.dws_traffic_keyword_window partition par#{date}\n" +
            "group by  keyword;")
    List<TrafficKeywordStatsBean> selectKeywordStats(Integer date);
    // 2. 获取各渠道会话数
    @Select("select sc_name,\n" +
            "       sum(sv_ct) sv_ct\n" +
            "from dws_traffic_sc_is_new_page_view_window\n" +
            "         partition (par#{date})\n" +
            "group by sc_name\n" +
            "order by sv_ct desc;")
    List<TrafficSvCt> selectSvCt(@Param("date") Integer date);

    // 3. 获取各渠道会话平均页面浏览数
    @Select("select sc_name,\n" +
            "       sum(pv_ct) / sum(sv_ct) pv_per_session\n" +
            "from dws_traffic_sc_is_new_page_view_window\n" +
            "         partition (par#{date})\n" +
            "group by sc_name\n" +
            "order by pv_per_session desc;")
    List<TrafficPvPerSession> selectPvPerSession(@Param("date") Integer date);

    // 4. 获取各渠道会话平均页面访问时长
    @Select("select sc_name,\n" +
            "       sum(dur_sum) / sum(sv_ct) dur_per_session\n" +
            "from dws_traffic_sc_is_new_page_view_window\n" +
            "         partition (par#{date})\n" +
            "group by sc_name\n" +
            "order by dur_per_session desc;")
    List<TrafficDurPerSession> selectDurPerSession(@Param("date") Integer date);
    // 5. 获取各渠道跳出率
    @Select("select sc_name,\n" +
            "       sum(if(pv_ct = 1, 1, 0))                                      as single_page_num,\n" +
            "       sum(sv_ct)                                                    as session_num,\n" +
            "       cast(sum(if(pv_ct = 1, 1, 0)) / sum(sv_ct) as decimal(16, 3)) as ujRate\n" +
            "from dws_traffic_sc_is_new_page_view_window\n" +
            "partition par#{date}\n" +
            "group by sc_name")
    List<TrafficUjRate> selectUjRate(@Param("date") Integer date);


}
