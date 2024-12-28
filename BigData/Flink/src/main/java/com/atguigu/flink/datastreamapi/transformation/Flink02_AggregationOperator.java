package com.atguigu.flink.datastreamapi.transformation;

/*
聚合算子
    max
    min
    maxBy
    minBy
    sum

要聚合，先keyBy
    Flink希望通过keyBy的方式将数据拆分，再通过并行的方式处理数据
* */

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink02_AggregationOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(3), WatermarkStrategy.noWatermarks(), "ds");
        //ds
        //        .map(event -> new Tuple2<>(event.getUrl(), 1L))
        //        .returns(Types.TUPLE(Types.STRING, Types.LONG))
        //        .keyBy(o -> o.f0)
        //        .sum(1).setParallelism(4)
        //        .print();

        ds
                .keyBy(Event::getUser)
                .maxBy("timestamp")
                .print();
        env.execute();
    }
}
