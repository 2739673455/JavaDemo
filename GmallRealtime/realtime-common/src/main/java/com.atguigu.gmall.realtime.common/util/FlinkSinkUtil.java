package com.atguigu.gmall.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.bean.TableProcessDwd;
import com.atguigu.gmall.realtime.common.constant.Constant;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class FlinkSinkUtil {
    public static KafkaSink<String> getKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.<String>builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //.setTransactionalIdPrefix("dwd" + System.currentTimeMillis())
                //.setProperty("transaction.timeout.ms", 1000 * 60 * 15 + "")
                .build();
    }

    public static KafkaSink<Tuple2<JSONObject, TableProcessDwd>> getKafkaSink() {
        return KafkaSink.<Tuple2<JSONObject, TableProcessDwd>>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(
                        (KafkaRecordSerializationSchema<Tuple2<JSONObject, TableProcessDwd>>) (tup2, context, aLong) -> {
                            JSONObject data = tup2.f0;
                            TableProcessDwd tableProcessDwd = tup2.f1;
                            String topic = tableProcessDwd.getSinkTable();
                            return new ProducerRecord<>(topic, data.toJSONString().getBytes());
                        }
                )
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //.setTransactionalIdPrefix("dwd" + System.currentTimeMillis())
                //.setProperty("transaction.timeout.ms", 1000 * 60 * 15 + "")
                .build();
    }

    // KafkaSink通用方法，不管流中是什么数据类型，都能通过该方法获取KafkaSink，需要自己实现KafkaRecordSerializationSchema的serialize方法
    public static <T> KafkaSink<T> getKafkaSink(KafkaRecordSerializationSchema<T> krs) {
        return KafkaSink.<T>builder()
                .setBootstrapServers(Constant.KAFKA_BROKERS)
                .setRecordSerializer(krs)
                //.setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //.setTransactionalIdPrefix("dwd" + System.currentTimeMillis())
                //.setProperty("transaction.timeout.ms", 1000 * 60 * 15 + "")
                .build();
    }

    public static DorisSink<String> getDorisSink(String tableName) {
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true"); // 每行一条 json 数据
        
        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes(Constant.DORIS_FE_NODES)
                                .setTableIdentifier("gmall." + tableName)
                                .setUsername("root")
                                .setPassword("aaaaaa")
                                .build()
                )
                .setDorisExecutionOptions(
                        DorisExecutionOptions.builder()
                                .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                                .setDeletable(false)
                                .setBufferCount(3) // 用于缓存stream load数据的缓冲条数: 默认 3
                                .setBufferSize(1024 * 1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                                .setMaxRetries(3)
                                .setStreamLoadProp(props)
                                .build()
                )
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
