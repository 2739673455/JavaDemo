package com.atguigu.flink.datastreamapi.transformation;

/*
reduce:
    归约算子，两两聚合，每次使用上次归约的结果和本次新到的数据进行归约，第一条数据不进行归约处理
* */

import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_ReduceOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.fromSource(SourceUtil.getDataGeneratorSource(3), WatermarkStrategy.noWatermarks(), "ds")
                .map(o -> new Tuple2<>(o.getUrl(), 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(o -> o.f0)
                .reduce((ReduceFunction<Tuple2<String, Long>>) (result, in) -> new Tuple2<>(result.f0, result.f1 + in.f1))
                .print();
        env.execute();
    }
}
