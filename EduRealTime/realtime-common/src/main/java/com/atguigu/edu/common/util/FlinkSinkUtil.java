package com.atguigu.edu.common.util;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.edu.common.bean.TableProcessDwd;
import com.atguigu.edu.common.constant.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.PreparedStatement;
import java.sql.SQLException;

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

    public static SinkFunction<JSONObject> getDorisSink(String tableName, String... columnNames) {
        StringBuilder sql = new StringBuilder();
        sql.append("insert into ").append(tableName).append(" values(");
        for (String columnName : columnNames) {
            sql.append("?,");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(")");
        return JdbcSink.sink(
                sql.toString(),
                new JdbcStatementBuilder<JSONObject>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, JSONObject obj) throws SQLException {
                        for (int i = 1; i <= columnNames.length; i++) {
                            preparedStatement.setObject(i, obj.get(columnNames[i - 1]));
                        }
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchIntervalMs(100L)
                        .withBatchSize(1)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withDriverName(Constant.MYSQL_DRIVER)
                        .withUrl(Constant.DORIS_JDBC_URL)
                        .withUsername(Constant.DORIS_USER_NAME)
                        .withPassword(Constant.DORIS_PASSWORD)
                        .build()
        );
    }
}
