package com.atguigu.bigdata.spark.core.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("WordCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        jsc.textFile("SparkDemo/data/text.txt", 3)
                .flatMap(str -> Arrays.asList(str.split(" ")).iterator())
                .groupBy(str -> str)
                .map(tuple -> new Tuple2(tuple._1, tuple._2.spliterator().estimateSize()))
                .collect().forEach(System.out::println);

        jsc.stop();
    }
}
