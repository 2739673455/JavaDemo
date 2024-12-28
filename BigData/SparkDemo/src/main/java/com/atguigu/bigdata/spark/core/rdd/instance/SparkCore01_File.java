package com.atguigu.bigdata.spark.core.rdd.instance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

public class SparkCore01_File {
    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("Spark Env");
        conf.set("spark.default.parallelism", "6");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = jsc.textFile("SparkDemo/data/text.txt", 8);
        String outputPath = "SparkDemo/data/output" + System.currentTimeMillis();
        rdd.saveAsTextFile(outputPath);
        jsc.stop();
        Display.display(outputPath);
    }
}