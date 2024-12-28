package com.atguigu.flink.sql.query;

import com.atguigu.flink.pojo.WaterSensor;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import static org.apache.flink.table.api.Expressions.*;

public class Flink09_TableAggregateFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<WaterSensor> ds = env.fromSource(SourceUtil.getWaterSensorGeneratorSource(2), WatermarkStrategy.noWatermarks(), "ds");
        Table table = streamTableEnv.fromDataStream(ds);
        // 注册函数
        streamTableEnv.createTemporaryFunction("top2func", Top2Func.class);
        // TableAPI
        table.groupBy($("id"))
                .flatAggregate(call("top2func", $("vc")))
                .select($("id"), $("rkvc"), $("rk"))
                .execute().print();
        // SQL 不支持
    }

    // 求每种传感器水位记录前2名
    @FunctionHint(output = @DataTypeHint(value = "ROW<rkvc bigint, rk integer>"))
    public static class Top2Func extends TableAggregateFunction<Row, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return Tuple2.of(Long.MIN_VALUE, Long.MIN_VALUE);
        }

        public void accumulate(Tuple2<Long, Long> acc, Long vc) {
            if (acc.f0 < vc) {
                acc.f1 = acc.f0;
                acc.f0 = vc;
            } else if (acc.f1 < vc) {
                acc.f1 = vc;
            }
        }

        public void emitValue(Tuple2<Long, Long> acc, Collector<Row> out) {
            out.collect(Row.of(acc.f0, 1));
            if (acc.f1 > Long.MIN_VALUE) {
                out.collect(Row.of(acc.f1, 2));
            }
        }
    }
}
