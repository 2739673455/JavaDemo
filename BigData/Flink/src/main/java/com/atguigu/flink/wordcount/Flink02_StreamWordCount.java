package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/*
 * 有界流处理 / 无界流处理 - DataStream
 * 1. 准备执行环境
 * 2. 读取数据(source)
 * 3. 对数据转换处理(transformation)
 * 4. 输出结果(sink)
 * 5. 启动执行
 * */
public class Flink02_StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        boundedStream(env);

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");
        unboundedStream(env, host, port);

        env.execute();
    }

    private static void boundedStream(StreamExecutionEnvironment env) {
        //有界数据流
        DataStreamSource<String> streamSource = env.readTextFile("D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\input\\word.txt");
        SingleOutputStreamOperator<Tuple2<String, Long>> flatMapDS = streamSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) {
                        Arrays.stream(s.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1L)));
                    }
                }
        );
        KeyedStream<Tuple2<String, Long>, Object> keyByDS = flatMapDS.keyBy(o -> o.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyByDS.sum(1);
        sum.print();
    }

    private static void unboundedStream(StreamExecutionEnvironment env, String host, int port) {
        //无界数据流
        DataStreamSource<String> unboundedStreamSource = env.socketTextStream(host, port);
        unboundedStreamSource
                .flatMap(
                        new FlatMapFunction<String, Tuple2<String, Long>>() {
                            @Override
                            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) {
                                Arrays.stream(s.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1L)));
                            }
                        }
                )
                .keyBy(o -> o.f0)
                .sum(1)
                .print();
    }
}
