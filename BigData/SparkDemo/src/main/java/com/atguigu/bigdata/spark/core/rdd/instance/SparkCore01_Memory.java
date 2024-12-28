package com.atguigu.bigdata.spark.core.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SparkCore01_Memory {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark Env");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        List<String> names = Arrays.asList(
                "zhangsan",
                "lisi",
                "wangwu",
                "zhaoliu",
                "zhaoliu",
                "qianqi",
                "suanba"
        );
        JavaRDD<String> rdd = jsc.parallelize(names, 4);
        String outputPath = "SparkDemo/data/output" + System.currentTimeMillis();
        rdd.saveAsTextFile(outputPath);
        jsc.stop();
        Display.display(outputPath);
    }
}