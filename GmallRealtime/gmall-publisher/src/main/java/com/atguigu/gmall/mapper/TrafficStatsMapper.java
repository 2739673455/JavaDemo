package com.atguigu.gmall.mapper;

import com.atguigu.gmall.bean.TrafficUvCt;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

public interface TrafficStatsMapper {

    @Select("select ch,sum(uv_ct) as uv from dws_traffic_vc_ch_ar_is_new_page_view_window partition par#{date} group by ch order by uv desc limit #{limit}")
    List<TrafficUvCt> selectChUvCt(@Param("date") Integer date, @Param("limit") Integer limit);
}
