package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

//流批一体
public class Flink03_BatchAndStreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //有界流
        DataStreamSource<String> streamSource = env.readTextFile("D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\input\\word.txt");
        //无界流
        //DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        streamSource.
                flatMap(
                        new FlatMapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) {
                                Arrays.stream(line.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1L)));
                            }
                        }
                )
                .keyBy(o -> o.f0)
                .sum(1)
                .print();
        env.execute();
    }
}
