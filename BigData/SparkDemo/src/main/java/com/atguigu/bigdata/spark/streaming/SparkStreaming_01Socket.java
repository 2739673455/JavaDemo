package com.atguigu.bigdata.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkStreaming_01Socket {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(2000));
        JavaReceiverInputDStream<String> dstream = jsc.socketTextStream("127.0.0.1", 9999);
        dstream.print();
        jsc.start();
        jsc.awaitTermination();
    }
}