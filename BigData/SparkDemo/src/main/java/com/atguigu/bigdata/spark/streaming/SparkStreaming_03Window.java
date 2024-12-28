package com.atguigu.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreaming_03Window {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(1000));

        JavaReceiverInputDStream<String> dstream = jsc.socketTextStream("localhost", 9999);
//        window需要两个参数: 窗口范围，窗口移动时间，都必须为采集时间的整数倍
//        print()类似RDD的行动算子
        JavaDStream<String> window = dstream.window(new Duration(6000), new Duration(3000));
        window.print();

        jsc.start();
        jsc.awaitTermination();
    }
}