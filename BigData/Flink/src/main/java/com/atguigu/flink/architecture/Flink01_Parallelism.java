package com.atguigu.flink.architecture;

/*
并行度
算子并行度: 每个算子执行时并行的task个数
作业并行度: 当前作业中所有算子中最大的并行度

设置并行度
1. idea中，若不明确指定并行度，使用当前处理器个数作为并行度
2. env.setParallelism(n)
3. 算子().setParallelism(n)
4. 集群配置文件中 parallelism.default: 1 来指定集群默认并行度
5. 提交作业时指定并行度 bin/flink run -p n
 * */

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class Flink01_Parallelism {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.socketTextStream("localhost", 9999)
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> Arrays.asList(line.split(" ")).forEach(word -> out.collect(new Tuple2<>(word, 1L))))
                .setParallelism(4)
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(value -> value.f0)
                .sum(1)
                .print();

        env.execute();
    }
}
