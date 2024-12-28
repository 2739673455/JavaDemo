package com.atguigu.flink.wordcount;

import com.atguigu.flink.pojo.WordCount;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class Flink05_WebUIWordCount {
    public static void main(String[] args) throws Exception {
        //创建配置对象
        Configuration configuration = new Configuration();
        //指定webui的地址和端口
        configuration.setString("rest.address", "127.0.0.1");
        configuration.setInteger("rest.port", 8008);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        streamSource
                .flatMap((String line, Collector<WordCount> collector) -> Arrays.stream(line.split(" ")).forEach(word -> collector.collect(new WordCount(word, 1L))))
                .returns(Types.POJO(WordCount.class))
                .keyBy(WordCount::getWord)
                .sum("count")
                .print();
        env.execute();
    }
}
