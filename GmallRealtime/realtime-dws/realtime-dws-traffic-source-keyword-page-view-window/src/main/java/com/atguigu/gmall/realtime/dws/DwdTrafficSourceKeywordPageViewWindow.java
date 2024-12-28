package com.atguigu.gmall.realtime.dws;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import com.atguigu.gmall.realtime.dws.function.Keyword;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwdTrafficSourceKeywordPageViewWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwdTrafficSourceKeywordPageViewWindow().start(10021, 4, "dwd_traffic_source_keyword_page_view_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {

        // 注册函数
        streamTableEnv.createTemporaryFunction("ik_analyze", Keyword.class);

        // 从page_log读数据
        streamTableEnv.executeSql(
                "create table page_log ( " +
                        "page map<string,string>, " +
                        "ts bigint, " +
                        "event_time as to_timestamp_ltz(ts,3), " +
                        "watermark for event_time as event_time " +
                        ") " +
                        SQLUtil.getKafkaDDL("dwd_traffic_page", "dwd_traffic_source_keyword_page_view_window")
        );

        // 开窗分组聚合
        //Table result = streamTableEnv.sqlQuery(
        //        "select    date_format(tumble_start (event_time, interval '5' second), 'yyyy-MM-dd HH:mm:ss') as start_time,\n" +
        //                "          date_format(tumble_end (event_time, interval '5' second), 'yyyy-MM-dd HH:mm:ss')   as end_time,\n" +
        //                "          date_format(tumble_start (event_time, interval '5' second), 'yyyy-MM-dd')          as `current_date`,\n" +
        //                "          keyword,\n" +
        //                "          count(*)                                                                           as cnt\n" +
        //                "from      (\n" +
        //                "          select    keyword,\n" +
        //                "                    event_time\n" +
        //                "          from      (\n" +
        //                "                    select    page['item'] as fullword,\n" +
        //                "                              event_time\n" +
        //                "                    from      page_log\n" +
        //                "                    where     page['last_page_id'] = 'search'\n" +
        //                "                    and       page['item_type'] = 'keyword'\n" +
        //                "                    and       page['item'] is not null\n" +
        //                "                    ) as search_table,\n" +
        //                "                    lateral table (ik_analyze (fullword)) t (keyword)\n" +
        //                "          ) as split_table\n" +
        //                "group by  keyword,\n" +
        //                "          tumble (event_time, interval '5' second)"
        //);

        Table result = streamTableEnv.sqlQuery(
                "with      split_table as (\n" +
                        "          select    keyword,\n" +
                        "                    event_time\n" +
                        "          from      (\n" +
                        "                    select    page['item'] as fullword,\n" +
                        "                              event_time\n" +
                        "                    from      page_log\n" +
                        "                    where     page['last_page_id'] = 'search'\n" +
                        "                    and       page['item_type'] = 'keyword'\n" +
                        "                    and       page['item'] is not null\n" +
                        "                    ) as search_table,\n" +
                        "                    lateral table (ik_analyze (fullword)) t (keyword)\n" +
                        "          )\n" +
                        "select    window_start,\n" +
                        "          window_end,\n" +
                        "          date_format(window_start, 'yyyy-MM-dd') as `current_date`,\n" +
                        "          keyword,\n" +
                        "          count(*) as keyword_count\n" +
                        "from      table (tumble (table split_table, descriptor (event_time), interval '5' second))\n" +
                        "group by  window_start,\n" +
                        "          window_end,\n" +
                        "          keyword;"
        );
        //result.execute().print();

        // 向Doris写入结果
        streamTableEnv.executeSql(
                "CREATE TABLE dws_traffic_source_keyword_page_view_window ( " +
                        "start_time TIMESTAMP(3), " +
                        "end_time TIMESTAMP(3), " +
                        "`current_date` string, " +
                        "keyword string, " +
                        "keyword_count bigint " +
                        ") " +
                        SQLUtil.getDorisDDL("dws_traffic_source_keyword_page_view_window")
        );
        result.executeInsert("dws_traffic_source_keyword_page_view_window");
    }
}
