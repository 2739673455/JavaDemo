package com.atguigu.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/*
 * 批处理 - DataSet
 * 1. 准备执行环境
 * 2. 读取数据(source)
 * 3. 对数据转换处理(transformation)
 * 4. 输出结果(sink)
 * */
public class Flink01_BatchWordCount {
    public static void main(String[] args) throws Exception {
        //1. 准备执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. 读取数据
        DataSource<String> dataSource = env.readTextFile("D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\input\\word.txt");
        //3. 对数据转换处理
        FlatMapOperator<String, Tuple2<String, Long>> flatMapDS = dataSource.flatMap(
                new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Long>> collector) {
                        Arrays.stream(s.split(" ")).forEach(word -> collector.collect(Tuple2.of(word, 1L)));
                    }
                }
        );
        //groupBy(int)    如果当前数据类型是Tuple，指定使用Tuple中的第几个元素作为分组的key
        //groupBy(String) 如果当前数据类型是POJO(简单理解为JavaBean)，指定使用POJO中的哪个属性作为分组的key
        UnsortedGrouping<Tuple2<String, Long>> groupByDS = flatMapDS.groupBy(0);
        AggregateOperator<Tuple2<String, Long>> sum = groupByDS.sum(1);
        sum.print();
    }
}
