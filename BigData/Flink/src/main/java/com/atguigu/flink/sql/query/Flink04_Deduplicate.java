package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/*
topN中rk=1的场景，要求使用时间属性进行排序，会识别为去重语法
* */
public class Flink04_Deduplicate {
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
        // 取每个窗口中每个url最后到达的数据
        //   将同一个窗口同一个url分到一起，排名，取第一名
        Table table1 = streamTableEnv.sqlQuery(
                "select " +
                        "url, " +
                        "event_time, " +
                        "window_start, " +
                        "window_end, " +
                        "row_number() over(partition by url, window_start order by event_time) as rk " +
                        "from table(tumble(table t1, descriptor(event_time), interval '2' second)) "
        );
        streamTableEnv.createTemporaryView("t2", table1);
        streamTableEnv.sqlQuery(
                "select url, event_time, window_start, window_end from t2 where rk=1"
        ).execute().print();
    }
}
