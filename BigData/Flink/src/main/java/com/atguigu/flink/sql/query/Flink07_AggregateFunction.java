package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class Flink07_AggregateFunction {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<WaterSensor> ds = env.fromSource(SourceUtil.getWaterSensorGeneratorSource(2), WatermarkStrategy.noWatermarks(), "ds");
        Table table = streamTableEnv.fromDataStream(ds);
        streamTableEnv.createTemporaryView("t1", table);
        // 注册函数
        streamTableEnv.createTemporaryFunction("avgfunc", AvgFunc.class);
        // TableAPI
        table.groupBy($("id")).select($("id"), call("avgfunc", $("vc"))).execute().print();
        // SQL
        streamTableEnv.sqlQuery("select id, avgfunc(vc) from t1 group by id").execute().print();
    }

    // 计算每种传感器平均水位
    public static class AvgFunc extends AggregateFunction<Double, Tuple2<Long, Long>> {

        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(0L, 0L);
        }

        public void accumulate(Tuple2<Long, Long> acc, Long vc) {
            acc.f0 = acc.f0 + vc;
            acc.f1 = acc.f1 + 1;
        }

        @Override
        public Double getValue(Tuple2<Long, Long> acc) {
            return (double) acc.f0 / acc.f1;
        }
    }
}