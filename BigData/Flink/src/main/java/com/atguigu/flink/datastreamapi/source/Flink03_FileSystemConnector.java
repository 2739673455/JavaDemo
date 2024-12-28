package com.atguigu.flink.datastreamapi.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/*
添加依赖:
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-files</artifactId>
        <version>${flink.version}</version>
        <scope>provided</scope>
    </dependency>

Source类型:
    SourceFunction(旧的) : addSource()
    Source : fromSource()
* */

public class Flink03_FileSystemConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //File Source 从文件中读取数据
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(
                new TextLineInputFormat(),
                new Path("D:\\Code\\JavaProject\\20240522java\\BigData0522\\Flink\\input\\word.txt")
        ).build();
        DataStreamSource<String> streamSource = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "fileSource");
        streamSource.print();
        env.execute();
    }
}
