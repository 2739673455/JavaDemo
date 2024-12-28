package com.atguigu.gmall.realtime.test;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.doris.flink.source.DorisSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Test02_Flink_Doris_API {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        // 从Doris读数据
        DorisSource<List<?>> dorisSource = DorisSource.<List<?>>builder()
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes("hadoop102:7030")
                                .setTableIdentifier("test_db.table1")
                                .setUsername("root")
                                .setPassword("aaaaaa")
                                .build()
                )
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDeserializer(new SimpleListDeserializationSchema())
                .build();
        //env.fromSource(dorisSource, WatermarkStrategy.noWatermarks(), "doris source").print();

        // 向Doris写数据
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        List<Tuple4<Integer, Integer, String, Integer>> data = new ArrayList<>();
        data.add(new Tuple4<>(1, 1, "jim", 7));
        DataStreamSource<Tuple4<Integer, Integer, String, Integer>> source = env.fromCollection(data);

        source.map((MapFunction<Tuple4<Integer, Integer, String, Integer>, String>) t -> t.f0 + "\t" + t.f1 + "\t" + t.f2 + "\t" + t.f3)
                .sinkTo(
                        DorisSink.<String>builder()
                                .setDorisReadOptions(DorisReadOptions.builder().build())
                                .setDorisExecutionOptions(
                                        DorisExecutionOptions.builder()
                                                .setLabelPrefix(System.currentTimeMillis() + "")
                                                .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                                                .setDeletable(false)
                                                .setBufferCount(3) // 用于缓存stream load数据的缓冲条数: 默认 3
                                                .setBufferSize(1024 * 1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                                                .setMaxRetries(3)
                                                .setStreamLoadProp(new Properties())
                                                .build()
                                )
                                .setSerializer(new SimpleStringSerializer())
                                .setDorisOptions(
                                        DorisOptions.builder()
                                                .setFenodes("hadoop102:7030")
                                                .setTableIdentifier("test_db.table1")
                                                .setUsername("root")
                                                .setPassword("aaaaaa")
                                                .build()
                                ).build()
                );

        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
