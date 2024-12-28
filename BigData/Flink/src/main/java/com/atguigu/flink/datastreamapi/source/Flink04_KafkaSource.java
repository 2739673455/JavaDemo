package com.atguigu.flink.datastreamapi.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/*
添加依赖:
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>${flink.version}</version>
        <scope>compile</scope>
    </dependency>

KafkaConnector:
    Kafka Source
    Kafka Sink
*/


public class Flink04_KafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //指定某分区从offset开始消费
        //HashMap<TopicPartition, Long> offsets = new HashMap<>();
        //offsets.put(new TopicPartition("topicA", 0), 28020L);

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("flink")
                .setTopics("topicA")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //基于提交的offset重新消费，若需要重置offset，重置到尾部
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //指定某分区从offset开始消费
                //.setStartingOffsets(OffsetsInitializer.offsets(offsets))
                //其他配置使用通用方法
                .setProperty("isolation.level", "read_committed")
                .build();
        DataStreamSource<String> streamSource = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");
        streamSource.print();

        env.execute();
    }
}
