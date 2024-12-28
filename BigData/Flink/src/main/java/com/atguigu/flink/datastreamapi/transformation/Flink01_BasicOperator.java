package com.atguigu.flink.datastreamapi.transformation;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/*
基本转换算子
    map: 映射，将输入的数据经过映射，输出一个数据
    filter: 过滤，将输入的数据经过过滤，满足条件才输出
    flatMap: 扁平映射，将输入的数据经过扁平映射，输出多个数据
 * */

public class Flink01_BasicOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(2), WatermarkStrategy.noWatermarks(), "dataGenSource");
        ds
                .filter(o -> "/home".equals(o.getUrl()))
                .flatMap((Event o, Collector<String> out) -> {
                    out.collect(o.getUser());
                    out.collect(o.getUrl());
                    out.collect(o.getTs().toString());
                })
                .returns(Types.STRING)
                .map(JSON::toJSONString)
                .print();
        env.execute();
    }
}
