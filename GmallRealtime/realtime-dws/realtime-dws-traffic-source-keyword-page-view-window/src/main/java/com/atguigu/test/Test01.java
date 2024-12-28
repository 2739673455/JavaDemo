package com.atguigu.test;

import com.atguigu.gmall.realtime.common.base.BaseSQLApp;
import com.atguigu.gmall.realtime.common.constant.Constant;
import com.atguigu.gmall.realtime.common.util.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Test01 extends BaseSQLApp {
    public static void main(String[] args) {
        new Test01().start(10099, 4, "test01");
    }

    @Override
    public void handle(StreamExecutionEnvironment env, StreamTableEnvironment streamTableEnv) {
        streamTableEnv.createTemporaryFunction("split_word", SplitWord.class);
        streamTableEnv.executeSql(
                "create table page_log (" +
                        "page map<string,string>, " +
                        "ts bigint, " +
                        "event_time as to_timestamp_ltz(ts,3), " +
                        "watermark for event_time as event_time " +
                        ")" +
                        SQLUtil.getKafkaDDL(Constant.TOPIC_DWD_TRAFFIC_PAGE, "test01")
        );
        Table table = streamTableEnv.sqlQuery(
                "with      split_table as (\n" +
                        "          select    keyword,\n" +
                        "                    event_time\n" +
                        "          from      (\n" +
                        "                    select    page['item'] as item,\n" +
                        "                              event_time\n" +
                        "                    from      page_log\n" +
                        "                    where     page['last_page_id'] = 'search'\n" +
                        "                    and       page['item_type'] = 'keyword'\n" +
                        "                    and       page['item'] is not null\n" +
                        "                    ) as search_table,\n" +
                        "                    lateral table (split_word (item)) as t (keyword)\n" +
                        "          )\n" +
                        "select    window_start,\n" +
                        "          window_end,\n" +
                        "          date_format(window_start, 'yyyy-MM-dd') as cur_date,\n" +
                        "          keyword,\n" +
                        "          count(*)                                as keyword_count\n" +
                        "from      table (tumble (table split_table, descriptor (event_time), interval '5' second))\n" +
                        "group by  window_start,\n" +
                        "          window_end,\n" +
                        "          keyword;"
        );

        //table.execute().print();
        streamTableEnv.executeSql(
                "create table doris_sink ( " +
                        "start_time timestamp(3), " +
                        "end_time timestamp(3), " +
                        "`current_date` string, " +
                        "keyword string, " +
                        "keyword_count bigint " +
                        ")" +
                        SQLUtil.getDorisDDL("dws_traffic_source_keyword_page_view_window")
        );
        table.executeInsert("doris_sink");
    }
}
