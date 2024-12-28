package com.atguigu.flink.datastreamapi.source;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class Flink01_Environment {
    public static void main(String[] args) throws Exception {
        //本地执行环境
        //LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();
        //System.out.println(localEnvironment);

        //远程执行环境
        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("hadoop102", 46413);
        remoteEnvironment
                .socketTextStream("hadoop102", 9999)
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> Arrays.asList(line.split(" ")).forEach(word -> out.collect(new Tuple2<>(word, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(o -> o.f0)
                .sum(1)
                .print();
        remoteEnvironment.execute();
    }
}
