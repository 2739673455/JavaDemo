package com.atguigu.flink.datastreamapi.sink;


import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
Kafka生产者
    相关配置:
        key和value序列化器 serializer
        集群地址 bootstrap-server
        应答级别 ack
        事务id
            transactional.id
        事务超时时间
            transactional.timeout.ms

使用KafkaSink , DeliveryGuarantee.EXACTLY_ONCE 一致性级别 注意事项:
    1. The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms)
        Kafka Broker 级别的超时时间:
            transaction.max.timeout.ms: 900000(15 minutes)
        Kafka Producer 级别的超时时间:
            transaction.timeout.ms: 60000(1 minutes)
            Flink KafkaSink 默认的事务超时时间: DEFAULT_KAFKA_TRANSACTION_TIMEOUT(transaction.timeout.ms) = Duration.ofHours(1);
                1 hour
    2. 设置事务id前缀
* */

public class Flink02_KafkaSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(2000L);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(1), WatermarkStrategy.noWatermarks(), "ds");
        KafkaSink<Event> kafkaSink = KafkaSink.<Event>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<Event>builder()
                                .setTopic("topicA")
                                .setValueSerializationSchema(
                                        new SerializationSchema<Event>() {
                                            @Override
                                            public byte[] serialize(Event event) {
                                                return event.toString().getBytes();
                                            }
                                        }
                                )
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
