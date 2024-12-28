package com.atguigu.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerClient {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        // 集群的地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        // key的序列化器
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // value的序列化器
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置自定义分区器
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 10; i++) {
            // 异步发送
            producer.send(new ProducerRecord<>("test", String.valueOf(i)));
            // 同步发送，加get()方法即可
            producer.send(new ProducerRecord<>("test", 0, String.valueOf(i), "111")).get();
            // 带回调函数
            producer.send(
                    new ProducerRecord<>("test", String.valueOf(i)),
                    new Callback() {
                        // 该方法在Producer收到ack时调用，为异步调用
                        @Override
                        public void onCompletion(RecordMetadata metadata, Exception exception) {
                            if (exception == null) // 没有异常,输出信息到控制台
                                System.out.println(metadata.topic() + "->" + metadata.partition());
                            else // 出现异常打印
                                exception.printStackTrace();
                        }
                    }
            );
        }
        producer.close();
    }

    // 自定义分区器，实现Partition接口，重写partition
    public static class MyPartitioner implements Partitioner {
        @Override
        public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
            return value.toString().hashCode() % 2;
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> map) {
        }
    }
}