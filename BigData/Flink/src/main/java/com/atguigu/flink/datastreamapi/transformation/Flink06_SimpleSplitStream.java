package com.atguigu.flink.datastreamapi.transformation;

/*
简单实现: 通过filter算子，从原始流中将满足条件的数据挑选出来放到新流中
* */

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_SimpleSplitStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(5), WatermarkStrategy.noWatermarks(), "ds");
        ds.filter(o -> "zhangsan".equals(o.getUser()))
                .print("zhangsan => ");
        ds.filter(o -> "lisi".equals(o.getUser()))
                .print("lisi => ");
        ds.filter(o -> !"zhangsan".equals(o.getUser()) && !"lisi".equals(o.getUser()))
                .print("other => ");

        env.execute();
    }
}
