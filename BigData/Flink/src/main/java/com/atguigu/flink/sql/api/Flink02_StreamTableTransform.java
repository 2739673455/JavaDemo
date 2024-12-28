package com.atguigu.flink.sql.api;

import com.atguigu.flink.pojo.WaterSensor;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/*
流表转换: 使用流表环境 StreamTableEnvironment
* */
public class Flink02_StreamTableTransform {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<WaterSensor> ds = env.fromSource(SourceUtil.getWaterSensorGeneratorSource(2), WatermarkStrategy.noWatermarks(), "ds");

        Table table = streamTableEnv.fromDataStream(ds);

        // 追加查询
        //  TableAPI
        Table appendTable = table.where(Expressions.$("vc").isGreaterOrEqual(100))
                .select(Expressions.$("id"), Expressions.$("vc"), Expressions.$("ts"));
        //  SQL
        //streamTableEnv.createTemporaryView("t1", table);
        //Table appendTable = streamTableEnv.sqlQuery("select * from t1");
        // 表转流
        DataStream<Row> dataStream = streamTableEnv.toDataStream(appendTable);
        dataStream.print();


        // 更新查询
        //  TableAPI
        //Table updateTable = table.groupBy(Expressions.$("id"))
        //        .select(Expressions.$("id"), Expressions.$("vc").sum().as("sum_vc"));
        //  SQL
        //streamTableEnv.createTemporaryView("t1", table);
        //Table updateTable = streamTableEnv.sqlQuery("select id,sum(vc) from t1 group by id");
        // 表转流
        //DataStream<Row> updateDs = streamTableEnv.toChangelogStream(updateTable);
        //updateDs.print();

        env.execute();
    }
}
