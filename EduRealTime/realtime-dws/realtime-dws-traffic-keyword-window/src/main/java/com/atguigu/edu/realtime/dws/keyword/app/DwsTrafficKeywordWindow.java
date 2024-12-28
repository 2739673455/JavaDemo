package com.atguigu.edu.realtime.dws.keyword.app;

import com.atguigu.edu.common.base.BaseSQLApp;
import com.atguigu.edu.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DwsTrafficKeywordWindow extends BaseSQLApp {
    public static void main(String[] args) {
        new DwsTrafficKeywordWindow().start(10111, 4, "dws_traffic_keyword_window");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {

        streamTableEnv.executeSql(
                "create table page_log( " +
                        "page map<string,string>, " +
                        "ts bigint, " +
                        "event_time as to_timestamp_ltz(ts,3), " +
                        "watermark for event_time as event_time " +
                        ")" +
                        SQLUtil.getKafkaSource("dwd_traffic_page", "dws_traffic_keyword_window")
        );
        Table result = streamTableEnv.sqlQuery(
                "with      keyword_table as (\n" +
                        "          select    page['item'] as keyword,\n" +
                        "                    event_time\n" +
                        "          from      page_log\n" +
                        "          where     page['item_type'] = 'keyword'\n" +
                        "          and       page['item'] is not null\n" +
                        "          )\n" +
                        "select    window_start,\n" +
                        "          window_end,\n" +
                        "          date_format(window_start, 'yyyy-MM-dd') as cur_date,\n" +
                        "          keyword,\n" +
                        "          count(*)                                as keyword_count\n" +
                        "from      table (tumble (table keyword_table, descriptor (event_time), interval '10' second))\n" +
                        "group by  window_start,\n" +
                        "          window_end,\n" +
                        "          keyword;"
        );
        streamTableEnv.executeSql(
                "create table dws_traffic_keyword_window( " +
                        "start_time TIMESTAMP(3), " +
                        "end_time TIMESTAMP(3), " +
                        "`current_date` string, " +
                        "keyword string, " +
                        "keyword_count bigint " +
                        ") " +
                        SQLUtil.getDorisSink("dws_traffic_keyword_window")
        );
        result.executeInsert("dws_traffic_keyword_window");
    }
}
