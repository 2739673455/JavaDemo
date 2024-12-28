package com.atguigu.bigdata.spark.core.rdd.function;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SparkCore01_Function_KV {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark RDD Function");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD<Integer> rdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 0), 10);
        List<Integer> okIds = new ArrayList<>();
        okIds.add(4);
        okIds.add(6);
        okIds.add(8);
        okIds.add(12);
        okIds.add(25);
        Broadcast<List<Integer>> broadcast = jsc.broadcast(okIds);
        JavaRDD<Integer> filterRDD = rdd.filter(
                num -> broadcast.value().contains(num)
        );
        filterRDD.collect().forEach(System.out::println);
        jsc.stop();
    }
}