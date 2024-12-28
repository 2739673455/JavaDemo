package com.atguigu.flink.checkpoint;

/*
Kafka Source -> Flink -> Kafka Sink
端到端精确一次
* */

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.time.LocalDateTime;

public class Flink02_KafkaFlinkKafkaEOS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        //指定某分区从offset开始消费
        //HashMap<TopicPartition, Long> offsets = new HashMap<>();
        //offsets.put(new TopicPartition("topicA", 0), 28020L);

        env.enableCheckpointing(2000L, CheckpointingMode.EXACTLY_ONCE); // 启用检查点，设置检查点保存为精准一次
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("flink")
                .setTopics("topicA")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                // 基于提交的offset重新消费，若需要重置offset，重置到尾部
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setProperty("isolation.level", "read_committed") // 设置隔离级别为读已提交
                .build();

        SingleOutputStreamOperator<String> ds = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource")
                .map(o -> o + " " + LocalDateTime.now());

        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic("topicA")
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE) // 设置精准一次
                .setProperty("transaction.timeout.ms", "600000") // 设置Producer事务超时时间
                .setTransactionalIdPrefix("flink-" + System.currentTimeMillis()) // 设置事务id前缀
                .build();
        ds.sinkTo(kafkaSink);

        env.execute();
    }
}
