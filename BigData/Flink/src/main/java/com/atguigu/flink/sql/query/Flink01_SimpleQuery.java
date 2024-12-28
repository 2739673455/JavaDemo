package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink01_SimpleQuery {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<WaterSensor> ds = env.fromSource(SourceUtil.getWaterSensorGeneratorSource(2), WatermarkStrategy.noWatermarks(), "ds");
        Schema schema = Schema.newBuilder()
                .column("id", "string")
                .column("vc", "bigint")
                .column("ts", "bigint")
                .columnByExpression("process_time", "proctime()")
                .columnByExpression("event_time", "to_timestamp_ltz(ts,3)")
                .watermark("event_time", "event_time-interval '1' second")
                .build();
        Table table = streamTableEnv.fromDataStream(ds, schema);
        streamTableEnv.createTemporaryView("t1", table);
        //streamTableEnv.sqlQuery("select * from t1").execute().print();
        streamTableEnv.sqlQuery("select distinct id from t1").execute().print();
        //streamTableEnv.sqlQuery("select id,vc,ts,event_time from t1 order by event_time").execute().print(); // 在流模式中，只能基于时间字段做升序排序
        //streamTableEnv.sqlQuery(""); // limit只支持批模式

        // sql hints
        //streamTableEnv.executeSql(
        //"create table t3(id string, vc bigint, ts bigint) " +
        //        "with(" +
        //        "'connector'='jdbc'," +
        //        "'driver'='com.mysql.cj.jdbc.Driver'," +
        //        "'url'='jdbc:mysql://localhost:3306/db1'," +
        //        "'table-name'='watersensor'," +
        //        "'username'='root'," +
        //        "'password'='123321'" +
        //        ")"
        //);
        //streamTableEnv.sqlQuery("select * from t3").execute().print();
        //streamTableEnv.sqlQuery("select * from t3 /*+ options('table-name'='watersensor3') */").execute().print();

        // 集合操作
        //streamTableEnv.executeSql("create view t4(s) as values ('c'),('a'),('b'),('b'),('c')");
        //streamTableEnv.executeSql("create view t5(s) as values ('d'),('e'),('a'),('b'),('b')");
        //  union / union all 并集
        //streamTableEnv.sqlQuery("(select * from t4) union (select * from t5)").execute().print();
        //streamTableEnv.sqlQuery("(select * from t4) union all (select * from t5)").execute().print();
        //  intersect / intersect all 交集
        //streamTableEnv.sqlQuery("(select * from t4) intersect (select * from t5)").execute().print();
        //streamTableEnv.sqlQuery("(select * from t4) intersect all (select * from t5)").execute().print();
        //  except / except all 差集
        //streamTableEnv.sqlQuery("(select * from t4) except (select * from t5)").execute().print();
        //streamTableEnv.sqlQuery("(select * from t4) except all (select * from t5)").execute().print();

        // 分组聚合
        //streamTableEnv.sqlQuery("select id,sum(vc) as sum_vc from t1 group by id").execute().print();
        // 多维分析
        //  需求: t_test表中有a,b,c,amount四个字段，其中a,b,c为维度字段。基于a,b,c三个维度字段的不同组合统计sum(amount)，所有结果在一张表中
        //  group by grouping sets (任意维度组合)
        //  例如: group by grouping sets (a, b, c, (a,b), (a,c), (b,c), (a,b,c), ())
        //  group by rollup (多个维度字段)
        //  例如: group by rollup (a, b, c)
        //    等价于 group by grouping sets ((a,b,c), (a,b), a, ())
        //  group by cube (多个维度字段)
        //  例如: group by cube (a, b, c)
        //    等价于 group by grouping sets (a, b, c, (a,b), (a,c), (b,c), (a,b,c), ())

        // grouping sets
        //streamTableEnv.sqlQuery(
        //        "select id,ts,count(*) cnt from" +
        //                "(" +
        //                "values" +
        //                "('1',1000,1)," +
        //                "('2',2000,2)," +
        //                "('3',3000,3)," +
        //                "('4',4000,4)," +
        //                "('1',5000,1)" +
        //                ")" +
        //                "as ws (id,ts,vc)" +
        //                "group by grouping sets(id,ts,(id,ts),())"
        //).execute().print();
        // grouping cube
        //streamTableEnv.sqlQuery(
        //        "select id,ts,count(*) cnt from" +
        //                "(" +
        //                "values" +
        //                "('1',1000,1)," +
        //                "('2',2000,2)," +
        //                "('3',3000,3)," +
        //                "('4',4000,4)," +
        //                "('1',5000,1)" +
        //                ")" +
        //                "as ws (id,ts,vc)" +
        //                "group by cube(id,ts)"
        //).execute().print();
        // grouping rollup
        //streamTableEnv.sqlQuery(
        //        "select id,ts,count(*) cnt from" +
        //                "(" +
        //                "values" +
        //                "('1',1000,1)," +
        //                "('2',2000,2)," +
        //                "('3',3000,3)," +
        //                "('4',4000,4)," +
        //                "('1',5000,1)" +
        //                ")" +
        //                "as ws (id,ts,vc)" +
        //                "group by rollup(id,ts)"
        //).execute().print();

        // Hive的多维分析
        // https://cwiki.apache.org/confluence/display/Hive/Enhanced+Aggregation%2C+Cube%2C+Grouping+and+Rollup
    }
}
