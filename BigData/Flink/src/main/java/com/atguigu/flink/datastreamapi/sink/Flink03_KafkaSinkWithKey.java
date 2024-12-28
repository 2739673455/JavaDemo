package com.atguigu.flink.datastreamapi.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/*
带key写入
* */

public class Flink03_KafkaSinkWithKey {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(1), WatermarkStrategy.noWatermarks(), "ds");
        KafkaSink<Event> kafkaSink = KafkaSink.<Event>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<Event>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(Event event, KafkaSinkContext kafkaSinkContext, Long aLong) {
                                String key = event.getUser();
                                String value = JSON.toJSONString(event);
                                return new ProducerRecord<>("topicA", key.getBytes(), value.getBytes());
                            }
                        }
                )
                .build();
        ds.sinkTo(kafkaSink);

        env.execute();
    }
}
