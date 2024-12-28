package com.atguigu.bigdata.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.ArrayList;
import java.util.HashMap;

public class SparkStreaming_02Kafka {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("HelloWorld");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(2000));
        // 创建配置参数
        HashMap<String, Object> kafkaconf = new HashMap<>();
        kafkaconf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        kafkaconf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaconf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaconf.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        kafkaconf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // 需要消费的主题
        ArrayList<String> topics = new ArrayList<>();
        topics.add("test");

        JavaInputDStream<ConsumerRecord<String, String>> directStream = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferBrokers(),
                ConsumerStrategies.Subscribe(topics, kafkaconf)
        );
        directStream.map(v1 -> v1.value()).print(100);
        jsc.start();
        jsc.awaitTermination();
    }
}