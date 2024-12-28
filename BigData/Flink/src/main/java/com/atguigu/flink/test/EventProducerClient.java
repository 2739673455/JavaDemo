package com.atguigu.flink.test;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class EventProducerClient {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final String[] users = {"zhangsan", "lisi", "wangwu", "tom", "jerry", "alice"};
        final String[] urls = {"/home", "/list", "/cart", "/order", "/pay"};
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            String user = users[RandomUtils.nextInt(0, users.length)];
            String url = urls[RandomUtils.nextInt(0, urls.length)];
            long timestamp = System.currentTimeMillis();
            String message = user + " " + url + " " + timestamp;
            producer.send(new ProducerRecord<>("topicA", message));
            Thread.sleep(100);
        }
        producer.close();
    }
}