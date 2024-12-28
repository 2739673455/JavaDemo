package com.atguigu.flink.datastreamapi.sink;

import com.atguigu.flink.pojo.Event;
import com.atguigu.flink.util.SourceUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.util.Objects;

public class Flink01_FileSystemSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启检查点
        env.enableCheckpointing(5000L);

        DataStreamSource<Event> ds = env.fromSource(SourceUtil.getDataGeneratorSource(30), WatermarkStrategy.noWatermarks(), "ds");
        SingleOutputStreamOperator<String> stringDS = ds.map(Objects::toString);
        FileSink<String> fileSink = FileSink.<String>forRowFormat(
                        new Path("D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\output"),
                        new SimpleStringEncoder<>()
                )
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(MemorySize.parse("3m")) //文件多大滚动
                                .withRolloverInterval(Duration.ofSeconds(10L)) // 滚动间隔
                                .withInactivityInterval(Duration.ofSeconds(5L)) // 文件非活跃滚动间隔
                                .build()
                ) // 文件滚动策略
                .withBucketAssigner(
                        new DateTimeBucketAssigner<>("yyyy-MM-dd hh-mm")
                ) // 目录滚动策略
                .withOutputFileConfig(
                        OutputFileConfig.builder()
                                .withPartPrefix("event") // 文件名前缀
                                .withPartSuffix(".log") // 文件名后缀
                                .build()
                ) // 文件名策略
                .withBucketCheckInterval(1000L) // 检查间隔
                .build();

        stringDS.sinkTo(fileSink);

        env.execute();
    }
}
