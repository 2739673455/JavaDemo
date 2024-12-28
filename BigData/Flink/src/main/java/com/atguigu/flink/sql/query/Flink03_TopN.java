package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink03_TopN {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(5), WatermarkStrategy.noWatermarks(), "ds");
        Schema schema = Schema.newBuilder()
                .column("user", "string")
                .column("url", "string")
                .column("ts", "bigint")
                .columnByExpression("process_time", "proctime()")
                .columnByExpression("event_time", "to_timestamp_ltz(ts,3)")
                .watermark("event_time", "event_time - interval '0' second")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("t1", table);

        // 计算出窗口内每个url的访问量
        Table table1 = streamTableEnv.sqlQuery(
                "select url, count(*) cnt, window_start, window_end " +
                        "from table(tumble(table t1, descriptor(event_time), interval '2' second)) " +
                        "group by url, window_start, window_end "
        );

        // 对窗口内url按访问量降序排序
        streamTableEnv.createTemporaryView("t2", table1);
        Table table2 = streamTableEnv.sqlQuery(
                "select url, cnt, window_start, window_end, " +
                        "row_number() over(partition by window_start, window_end order by cnt desc) as rk " +
                        "from t2 "
        );

        // 选出访问量前2的url
        streamTableEnv.createTemporaryView("t3", table2);
        streamTableEnv.sqlQuery(
                "select url, cnt, window_start, window_end, rk from t3 where rk<=2 "
        ).execute().print();

        //streamTableEnv.sqlQuery(
        //        "select " +
        //                "url, " +
        //                "cnt, " +
        //                "window_start, " +
        //                "window_end, " +
        //                "rk " +
        //                "from " +
        //                "( " +
        //                "select " +
        //                "url, " +
        //                "cnt, " +
        //                "window_start, " +
        //                "window_end, " +
        //                "row_number() over ( " +
        //                "partition by " +
        //                "window_start, " +
        //                "window_end " +
        //                "order by " +
        //                "cnt desc " +
        //                ") as rk " +
        //                "from " +
        //                "( " +
        //                "select " +
        //                "url, " +
        //                "count(*)     as cnt, " +
        //                "window_start, " +
        //                "window_end " +
        //                "from " +
        //                "table (tumble (table t1, descriptor (event_time), interval '2' second)) " +
        //                "group by " +
        //                "url, " +
        //                "window_start, " +
        //                "window_end " +
        //                ") as t2 " +
        //                ") t3 " +
        //                "where " +
        //                "rk <= 2"
        //).execute().print();
    }
}
